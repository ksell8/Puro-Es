# Puro Español

This project was made because I am fucking annoyed with streaming providers not allowing me to filter on audio languages available.

I'm trying to learn Spanish.  All my devices are in Spanish.  And the streaming providers put the titles in Spanish when there is no Spanish audio or Spanish captions available.  It drives me goddamn bonkers.  What's the fucking point in putting a title in Spanish when the MOVIE IS NOT IN SPANISH.

I digress.  This was made with **Claude**. The plan is to clean it up so it can be updated automatically once per week.  The basis for Claude's work is in .claude.  I additionally yelled at it to make performance tweaks and pull from the jsons instead of re-gathering data we already had.  Time to complete initial prototype: 3 hrs.  I have no clue how it made shit pretty, that's not my job.

### Decisions that need to be made for this shit to scale

1) Are we going to regenerate the data each time or cache it?
2) If the former, each streaming service should be broken up so they can maybe run in lambdas (space/time limitations will be difficult there).
3) If the latter, a database is probably necessary rather than loading everything in memory, and we will also need to figure out how to remove stale data (shit that is no longer streaming).

### Cool Improvements

1) Allowing non-English original dubs.  Right now I went with English for a limited scope.  I'm limited to 1000 calls/month with WatchMode.
2) Figure out how to view the full description.

### How to see the site

Locally run: python3 -m http.server 9000

and then go to http://localhost:9000 (lol)

Or go to: https://trippyhippies.org/puro-es

### How to reuse the scripts for your lang of choice

Edit the `es` to be what the fuck you want.  Delete my `data/titles.json`.

Then:

    uv sync
    uv run scripts/update.py

## APIs Used

Big thanks to the two API providers who made this shit fly.  Without them, I would've had to scrape the big boys.

[WatchMode API](https://api.watchmode.com/docs)

[TMDB API](https://developer.themoviedb.org/reference/getting-started)
