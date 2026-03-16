"""
Puro Español data pipeline.

Fetches Spanish-language streaming titles from Watchmode and enriches
them with metadata and poster paths from TMDB. Writes:
  - data/titles.json
  - data/meta.json

Can run locally or as an AWS Lambda function. When S3_BUCKET is set,
output is written to S3 instead of the local filesystem.

Env vars required: WATCHMODE_API_KEY, TMDB_API_KEY
Env vars optional: S3_BUCKET (if set, write output to S3)
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

WATCHMODE_API_KEY  = os.environ.get("WATCHMODE_API_KEY")
TMDB_BEARER_TOKEN  = os.environ.get("TMDB_BEARER_TOKEN")

if not WATCHMODE_API_KEY:
    sys.exit("WATCHMODE_API_KEY environment variable is not set.")
if not TMDB_BEARER_TOKEN:
    sys.exit("TMDB_BEARER_TOKEN environment variable is not set.")

WATCHMODE_BASE = "https://api.watchmode.com/v1"
TMDB_BASE = "https://api.themoviedb.org/3"

# Services we care about (display name -> Watchmode name fragment for matching)
TARGET_SERVICES = [
    "Netflix",
    "Disney+",
    "Prime Video",
    "Max",
    "Peacock",
]

# Watchmode slug/key for each service (used to normalise the "services" field)
SERVICE_SLUG = {
    "Netflix": "netflix",
    "Disney+": "disney-plus",
    "Prime Video": "prime",
    "Max": "max",
    "Peacock": "peacock",
    "Apple TV+": "apple-tv-plus",
}

# TMDB rate limit is ~50 req/s; use a small delay to stay safe
TMDB_SLEEP = 0.02

# Where to write output (relative to repo root, used when not writing to S3)
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"

S3_BUCKET = os.environ.get("S3_BUCKET")

GENRE_MAP_CACHE = DATA_DIR / "genre_map.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MAX_RETRIES = 5
RETRY_BASE  = 2.0  # seconds — doubles each attempt


def _get_with_backoff(url: str, params: dict, headers: dict = None) -> dict | list:
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code == 429:
                wait = RETRY_BASE ** attempt
                print(f"  Rate limited — retrying in {wait:.0f}s …")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES - 1:
                raise
            wait = RETRY_BASE ** attempt
            print(f"  Request error ({exc}) — retrying in {wait:.0f}s …")
            time.sleep(wait)


def watchmode_get(path: str, params: dict = None) -> dict | list:
    params = params or {}
    params["apiKey"] = WATCHMODE_API_KEY
    return _get_with_backoff(f"{WATCHMODE_BASE}{path}", params)


def tmdb_get(path: str, params: dict = None) -> dict:
    return _get_with_backoff(
        f"{TMDB_BASE}{path}",
        params or {},
        headers={"Authorization": f"Bearer {TMDB_BEARER_TOKEN}", "accept": "application/json"},
    )


# ---------------------------------------------------------------------------
# Step 1 — resolve Watchmode source IDs
# ---------------------------------------------------------------------------


SOURCE_ID_CACHE = DATA_DIR / "source_ids.json"


def get_source_ids() -> dict[str, int]:
    """
    Return {display_name: source_id} for each TARGET_SERVICE.
    Reads from source_ids.json cache if present; otherwise fetches from
    Watchmode and writes the cache for next time.
    """
    if SOURCE_ID_CACHE.exists():
        cached = json.loads(SOURCE_ID_CACHE.read_text())
        print(f"Using cached source IDs ({SOURCE_ID_CACHE})")
        return cached

    print("Fetching Watchmode sources …")
    sources = watchmode_get("/sources/")

    name_map: dict[str, dict] = {src["name"].lower(): src for src in sources}

    service_ids: dict[str, int] = {}
    for name in TARGET_SERVICES:
        key = name.lower()
        if key in name_map:
            service_ids[name] = name_map[key]["id"]
        else:
            match = next(
                (v for k, v in name_map.items() if key in k or k in key), None
            )
            if match:
                service_ids[name] = match["id"]
            else:
                print(f"  WARNING: could not find source_id for '{name}'")

    print("Source IDs found:")
    for name, sid in service_ids.items():
        print(f"  {name}: {sid}")

    SOURCE_ID_CACHE.parent.mkdir(parents=True, exist_ok=True)
    SOURCE_ID_CACHE.write_text(json.dumps(service_ids, indent=2))
    print(f"Cached source IDs to {SOURCE_ID_CACHE}")

    return service_ids


# ---------------------------------------------------------------------------
# Step 2 — list titles per service
# ---------------------------------------------------------------------------


def list_titles_for_source(source_id: int, service_name: str) -> list[dict]:
    """
    Paginate through Watchmode list-titles filtered by source_id and
    languages ES+EN. Returns raw Watchmode title objects.
    """
    all_titles: list[dict] = []
    page = 1

    while True:
        print(f"  Fetching page {page} for {service_name} (source_id={source_id}) …")
        params = {
            "source_ids": source_id,
            "languages": "ES,EN",
            "types": "movie,tv_series",
            "limit": 250,
            "page": page,
        }
        try:
            data = watchmode_get("/list-titles/", params=params)
        except requests.HTTPError as exc:
            print(f"  HTTP error on page {page}: {exc} — stopping pagination")
            break

        titles = data.get("titles", [])
        all_titles.extend(titles)
        total_pages = data.get("total_pages", 1)
        print(f"    Got {len(titles)} titles (page {page}/{total_pages}, total so far: {len(all_titles)})")

        if page >= total_pages:
            break

        page += 1
        time.sleep(0.5)  # be polite to Watchmode

    return all_titles


# ---------------------------------------------------------------------------
# Step 3 — fetch TMDB metadata
# ---------------------------------------------------------------------------


def get_genre_map() -> dict[int, str]:
    """
    Return a genre id->name map. Reads from data/genre_map.json if present,
    otherwise fetches from TMDB and writes the cache.
    """
    if GENRE_MAP_CACHE.exists():
        raw = json.loads(GENRE_MAP_CACHE.read_text())
        print(f"Using cached genre map ({GENRE_MAP_CACHE})")
        return {int(k): v for k, v in raw.items()}

    print("Fetching TMDB genre lists …")
    movie_genres = tmdb_get("/genre/movie/list").get("genres", [])
    tv_genres    = tmdb_get("/genre/tv/list").get("genres", [])
    genre_map = {g["id"]: g["name"] for g in movie_genres + tv_genres}

    GENRE_MAP_CACHE.parent.mkdir(parents=True, exist_ok=True)
    GENRE_MAP_CACHE.write_text(json.dumps(genre_map, indent=2))
    print(f"Cached genre map to {GENRE_MAP_CACHE}")

    return genre_map


def fetch_tmdb_metadata(tmdb_id: int, tmdb_type: str) -> dict | None:
    """
    Fetch rich metadata from TMDB for a single title.
    tmdb_type: "movie" | "show"  (Watchmode uses "show" for TV series)
    Returns a normalised metadata dict or None on failure.
    """
    is_tv = tmdb_type in ("show", "tv_series", "tv")
    endpoint = f"/tv/{tmdb_id}" if is_tv else f"/movie/{tmdb_id}"

    try:
        data = tmdb_get(endpoint)
    except requests.HTTPError as exc:
        print(f"    TMDB error for {endpoint}: {exc}")
        return None

    genres = [g["name"] for g in data.get("genres", [])]

    if not is_tv:
        title = data.get("title") or data.get("original_title", "")
        year_raw = data.get("release_date", "")
        content_type = "movie"
    else:
        title = data.get("name") or data.get("original_name", "")
        year_raw = data.get("first_air_date", "")
        content_type = "series"

    year = int(year_raw[:4]) if year_raw and len(year_raw) >= 4 else None

    return {
        "title": title,
        "year": year,
        "type": content_type,
        "genres": genres,
        "imdb_rating": data.get("vote_average"),
        "original_language": data.get("original_language"),
        "plot_overview": data.get("overview", ""),
        "poster_path": data.get("poster_path"),
        "tmdb_id": tmdb_id,
        "tmdb_type": content_type,
    }


def fetch_latin_spanish_translation(tmdb_id: int, tmdb_type: str) -> dict | None:
    """
    Fetch TMDB translations and return the best Latin American Spanish
    translation data dict (title/overview fields), or None if no qualifying
    Spanish translation exists.

    Preference: MX > any non-ES Spanish > skip (Spain-only = excluded).
    """
    is_tv = tmdb_type in ("show", "tv_series", "tv")
    endpoint = f"/tv/{tmdb_id}/translations" if is_tv else f"/movie/{tmdb_id}/translations"

    try:
        data = tmdb_get(endpoint)
    except requests.HTTPError as exc:
        print(f"    TMDB translations error for {endpoint}: {exc}")
        return None

    es_translations = [t for t in data.get("translations", []) if t.get("iso_639_1") == "es"]
    if not es_translations:
        return None

    mx = next((t for t in es_translations if t.get("iso_3166_1") == "MX"), None)
    if mx:
        return mx["data"]

    non_spain = next((t for t in es_translations if t.get("iso_3166_1") != "ES"), None)
    if non_spain:
        return non_spain["data"]

    return None  # Spain-only — exclude


# ---------------------------------------------------------------------------
# Step 4 — deduplicate and merge services
# ---------------------------------------------------------------------------


def merge_titles(per_service: dict[str, list[dict]]) -> list[dict]:
    """
    per_service: {service_name: [watchmode_title_obj, ...]}

    For each unique tmdb_id, merge service lists, then enrich with TMDB data.
    Returns list of final title dicts.
    """
    # tmdb_id -> {watchmode_obj, services: [slug, ...]}
    merged: dict[int, dict] = {}
    no_tmdb: dict[int, dict] = {}  # watchmode_id -> record

    for service_name, titles in per_service.items():
        slug = SERVICE_SLUG[service_name]
        for t in titles:
            tid = t.get("tmdb_id")
            wid = t["id"]
            if not tid:
                if wid not in no_tmdb:
                    no_tmdb[wid] = {
                        "watchmode_id": wid,
                        "title": t.get("title", ""),
                        "year": t.get("year"),
                        "type": "series" if t.get("type") == "tv_series" else "movie",
                        "genres": [],
                        "imdb_rating": t.get("imdb_rating"),
                        "original_language": None,
                        "plot_overview": "",
                        "poster_path": None,
                        "services": [slug],
                    }
                else:
                    if slug not in no_tmdb[wid]["services"]:
                        no_tmdb[wid]["services"].append(slug)
                continue
            if tid not in merged:
                merged[tid] = {"watchmode": t, "services": [slug]}
            else:
                if slug not in merged[tid]["services"]:
                    merged[tid]["services"].append(slug)

    if no_tmdb:
        print(f"  {len(no_tmdb)} titles had no tmdb_id — using Watchmode data only")

    print(f"\nUnique titles (by tmdb_id): {len(merged)}")

    # Load existing titles to avoid re-fetching TMDB data
    existing_tmdb: dict[int, dict] = {}
    existing_wm: dict[int, dict] = {}
    if (DATA_DIR / "titles.json").exists():
        for t in json.loads((DATA_DIR / "titles.json").read_text()):
            if t.get("tmdb_id"):
                existing_tmdb[t["tmdb_id"]] = t
            elif t.get("watchmode_id"):
                existing_wm[t["watchmode_id"]] = t

    cached_count = 0
    fetched_count = 0
    for i, (tmdb_id, entry) in enumerate(merged.items(), start=1):
        wm = entry["watchmode"]
        tmdb_type = wm.get("tmdb_type", "movie")

        if i % 50 == 0:
            print(f"  Progress: {i}/{len(merged)} …")

        if tmdb_id in existing_tmdb:
            meta = existing_tmdb[tmdb_id]
            cached_count += 1
        else:
            # Fetch metadata and translations concurrently
            with ThreadPoolExecutor(max_workers=2) as pool:
                fut_meta  = pool.submit(fetch_tmdb_metadata, tmdb_id, tmdb_type)
                fut_trans = pool.submit(fetch_latin_spanish_translation, tmdb_id, tmdb_type)
                meta  = fut_meta.result()
                trans = fut_trans.result()

            if meta is None:
                content_type = "series" if wm.get("type") == "tv_series" else "movie"
                meta = {
                    "title": wm.get("title", ""),
                    "year": wm.get("year"),
                    "type": content_type,
                    "genres": [],
                    "imdb_rating": wm.get("imdb_rating"),
                    "original_language": None,
                    "plot_overview": "",
                    "poster_path": None,
                    "tmdb_id": tmdb_id,
                    "tmdb_type": tmdb_type,
                }
            fetched_count += 1
            time.sleep(TMDB_SLEEP)

            # For non-Spanish-originals, require a Latin American Spanish
            # translation; skip Spain-only or untranslated titles.
            if meta.get("original_language") != "es":
                if trans is None:
                    continue
                if trans.get("overview"):
                    meta["plot_overview"] = trans["overview"]

        meta["services"] = entry["services"]
        yield meta

    print(f"  {cached_count} from cache, {fetched_count} fetched from TMDB")

    # no-tmdb titles — use cached record if available, updating services
    for wid, record in no_tmdb.items():
        if wid in existing_wm:
            existing_wm[wid]["services"] = record["services"]
            yield existing_wm[wid]
        else:
            yield record


# ---------------------------------------------------------------------------
# Step 5 — write output
# ---------------------------------------------------------------------------


def write_output(titles_iter) -> None:
    import tempfile

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    titles_path = DATA_DIR / "titles.json"

    # Stream titles into a temp file, then atomically replace titles.json
    count = 0
    tmp = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=DATA_DIR, delete=False, suffix=".tmp"
    )
    try:
        tmp.write("[\n")
        first = True
        for title in titles_iter:
            if not first:
                tmp.write(",\n")
            tmp.write(json.dumps(title, ensure_ascii=False))
            first = False
            count += 1
        tmp.write("\n]")
        tmp.flush()
    finally:
        tmp.close()

    Path(tmp.name).replace(titles_path)
    print(f"\nWrote {count} titles to {titles_path}")

    now = datetime.utcnow()
    meta = {"updated": now.strftime("%B %Y"), "total": count}
    meta_json = json.dumps(meta, ensure_ascii=False, indent=2)

    if S3_BUCKET:
        import boto3
        s3 = boto3.client("s3")
        s3.upload_file(str(titles_path), S3_BUCKET, "data/titles.json",
                       ExtraArgs={"ContentType": "application/json"})
        print(f"Uploaded to s3://{S3_BUCKET}/data/titles.json")
        s3.put_object(Bucket=S3_BUCKET, Key="data/meta.json",
                      Body=meta_json.encode("utf-8"), ContentType="application/json")
        print(f"Wrote meta to s3://{S3_BUCKET}/data/meta.json")
    else:
        meta_path = DATA_DIR / "meta.json"
        meta_path.write_text(meta_json, encoding="utf-8")
        print(f"Wrote meta to {meta_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=== Puro Español data update ===\n")

    # Step 1: resolve source IDs
    source_ids = get_source_ids()

    # Step 2: list titles per service
    per_service: dict[str, list[dict]] = {}
    for service_name in TARGET_SERVICES:
        sid = source_ids.get(service_name)
        if sid is None:
            print(f"Skipping {service_name} (no source_id found)")
            continue
        print(f"\nFetching titles for {service_name} …")
        titles = list_titles_for_source(sid, service_name)
        per_service[service_name] = titles
        print(f"  Total for {service_name}: {len(titles)}")

    # Step 3 & 4: deduplicate and enrich with TMDB
    final_titles = merge_titles(per_service)

    # Step 5: write output
    write_output(final_titles)

    print("\nDone.")


def lambda_handler(event, context):
    """AWS Lambda entry point."""
    main()
    return {"statusCode": 200, "body": "Update complete"}


if __name__ == "__main__":
    main()
