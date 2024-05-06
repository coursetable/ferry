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

- `to_table.py`: this is a preparatory step that converts `parsed_evaluations` to 4 giant CSV files in `evaluation_tables`: `evaluation_questions.csv`, `evaluation_ratings.csv`, `evaluation_narratives.csv`, `evaluation_statistics.csv`. It's probably for performance reasons.
- `import_{courses,evaluations}.py`: imports the parsed data from `parsed_courses` and `evaluation_tables` into Pandas DataFrames. It does surface-level analysis such as deduplication, generating IDs, etc.
- `transform_compute.py`: generates analysis, such as average ratings, finding last offered courses, etc.

The analyzer creates the following tables in `importer_dumps`: `seasons.csv`, `courses.csv`, `listings.csv`, `course_professors.csv`, `professors.csv`, `course_flags.csv`, `flags.csv`, `evaluation_narratives.csv`, `evaluation_ratings.csv`, `evaluation_statistics.csv`, `evaluation_questions.csv`.

## Database connector

The database connector code is located in `ferry/database`.

- `stage.py`: stages the data in the database.
- `deploy.py`: checks table invariants, and regenerates the database based on the staged data. At this step we also run SQL to generate some derived tables.

TODO:

1. Move the SQL logic to the data analyzer.
2. Ferry should not regenerate the whole DB each time.
