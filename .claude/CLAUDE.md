# Puro Español

A static website listing Spanish-language streaming content (originals and dubbed) across major services, aimed at Latin American Spanish learners. Updated via a periodic script that hits two APIs and writes static JSON. The site is fully client-side — no server, just HTML/JS reading local JSON.

## Data pipeline

Two APIs are used together:

### 1. Watchmode API
- **Purpose**: Get the list of titles available in Spanish on each service
- **Free tier**: 1,000 calls/month
- **Key endpoint**: List Titles — returns `id`, `title`, `year`, `type`, `imdb_id`, `tmdb_id`, `tmdb_type` only (no rich metadata)
- **Filter**: Spanish audio availability — captures both originals and dubbed content in one call
- **Max per page**: 250 titles
- **Calls per update**: 1 per service = 7 calls/update
- **Update frequency**: Weekly (28 calls/month — well within budget)
- **No detail calls** — Watchmode is only used for the title list + tmdb_id

### 2. TMDB API (The Movie Database)
- **Purpose**: Get rich metadata and poster images for each title
- **Free tier**: Generous, no meaningful call limit for non-commercial use
- **Key endpoint**: `/find/{tmdb_id}?external_source=tmdb_id` — results are nested under `movie_results[0]` or `tv_results[0]`. Returns `genre_ids` (integers, not names) — map via `/genre/movie/list` and `/genre/tv/list`. No `runtime` field.
- **Poster path**: `https://image.tmdb.org/t/p/w342{poster_path}` for cards, `https://image.tmdb.org/t/p/w780{poster_path}` for hero
- **Attribution required**: See below

## Services (7 total)

Look up Watchmode `source_id` for each via `/v1/sources/` endpoint and fill in:

| Service     | Watchmode source_id |
|-------------|---|
| Netflix     | TBD |
| Disney+     | TBD |
| Prime Video | TBD |
| Max         | TBD |
| Hulu        | TBD |
| Peacock     | TBD |
| Apple TV+   | TBD |
| Tubi        | TBD |

## Language / content focus

- **Latin American Spanish only** — exclude or flag Castilian/Spain content
- **Dubbed content is explicitly in scope** — do not filter it out
- Use `original_language` field from TMDB to distinguish originals (`"es"`) from dubs (anything else)
- Mexican Spanish preferred where distinguishable

## Data model

One combined JSON file. Each title:

```json
{
  "title": "Narcos: México",
  "year": 2018,
  "type": "series",
  "genres": ["Crime", "Drama"],
  "imdb_rating": 8.0,
  "original_language": "en",
  "plot_overview": "Traces the rise of the Guadalajara cartel...",
  "services": ["netflix"],
  "poster_path": "/path/to/poster.jpg"
}
```

### Field notes
- `type`: `"movie"` or `"series"`
- `original_language`: ISO 639-1 code — `"es"` = Spanish original, anything else = dubbed
- `availability`: `"subscription"` or `"free"` only — exclude rent/buy titles
- `genres`: array of strings, mapped from TMDB `genre_ids` via `/genre/movie/list` and `/genre/tv/list`
- `poster_path`: raw path from TMDB, construct full URL at render time
- Note: `runtime` is not available from the `/find/` endpoint — omitted from data model

## Site features

- Service filter (tabs): Netflix, Disney+, Prime Video, Max, Hulu, Peacock, Apple TV+
- Filter by type: movies / series
- Filter by genre
- Filter by availability: subscription vs free
- Separate carousels for movies and series
- Poster cards (w342) with hover overlay showing: genre, title, year, runtime, description, IMDb rating, original/dubbed badge
- "Free" badge on poster corner for free-tier titles
- No URLs or deep links anywhere — users find content on their own
- No framework required — vanilla HTML/JS reading local JSON

## Design notes

- Text-only overlay on hover (dark semi-transparent overlay on poster)
- Badge colors: purple for ES originals, teal for dubbed
- Green (#1D9E75) as primary accent color
- Scroll buttons on carousels
- Mobile-friendly

## Attribution (required)

### TMDB
- Use official logo from https://www.themoviedb.org/about/logos-attribution — download and serve as local asset, do not hotlink
- Required notice: "This product uses the TMDB API but is not endorsed or certified by TMDB."
- Logo must link to themoviedb.org
- Attribution in footer of every page

### Watchmode
- Required notice: "Streaming data powered by Watchmode.com"
- "Watchmode.com" must be a hyperlink to https://watchmode.com
- Attribution in footer of every page

### Footer also includes
- "Updated [Month Year]" timestamp generated at build time

## Deployment

- **Static site**: S3 bucket with static website hosting enabled. All HTML/JS/CSS/assets and `data/` JSON served from S3.
- **Data pipeline**: `scripts/update.py` runs as an AWS Lambda function on a weekly schedule (EventBridge cron).
- **Lambda config**: Set `S3_BUCKET` env var to the bucket name. When set, the script writes `data/titles.json` and `data/meta.json` directly to S3 via boto3 instead of local disk.
- **API keys**: Injected as Lambda environment variables from a secrets manager.
- **Local dev**: Omit `S3_BUCKET` and the script writes to `data/` locally. Serve the site with any static file server (e.g. `python -m http.server`).

## No images from streaming services
- Do NOT use Netflix/Disney+/etc. title card images — these are copyrighted
- TMDB poster images are licensed for use with attribution (see above)