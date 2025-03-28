# Code/data structure

Ferry is split into three main parts:

- Crawler
- Data analyzer
- Database connector

## Crawler

The crawler code is located in `ferry/crawler`. It is further split into two crawlers:

- `classes`: fetches class data from the YCS API. Runs every day.
- `evals`: fetches eval data from the OCE API. Runs on demand only.

Each crawler has two parts, a `fetch.py` and a `parse.py`:

- `classes/fetch.py`: fetches class lists and dumps into `season_courses/{season}.json` or `season_courses/{season}_fysem.json`; fetches course details and dumps into `course_json_cache/{season}.json`.
- `classes/parse.py`: parses the fetched data (into a structure we can better utilize, including HTML sanitization, etc.) and dumps into `parsed_courses/{season}.json`.
- `evals/fetch.py`: fetches eval pages and dumps into `rating_cache/questions_index/{season}_{crn}.html`.
- `evals/parse.py`: parses the fetched eval pages and dumps into `parsed_evaluations/{season}-{crn}.json`.

## Data analyzer

The data analyzer code is located in `ferry/transform`. In this part, we transform JSON into a Pandas DataFrame, and then create some analyzed fields. The goal is to produce tables that exactly match the database schema.

- `import_{courses,evaluations}.py`: imports the parsed data from `parsed_courses` and `parsed_evaluations` into Pandas DataFrames. It does surface-level analysis such as deduplication, generating IDs, etc.
- `transform_compute.py`: generates analysis, such as average ratings, finding last offered courses, etc.

If the `--snapshot-tables` argument is used, the analyzer will create a CSV file for each DB table in `importer_dumps`.

## Database connector

The database connector code is located in `ferry/database`. It takes the Pandas tables and imports them into the database.
