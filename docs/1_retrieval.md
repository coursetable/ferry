# Retrieval

## Previous CourseTable files

The current CourseTable implementation stores classes by term in JSON files at publicly-available URLs. To compare our new crawler against these files, we have to first download these from:

- `https://coursetable.com/gen/json/data_<TERM_CODE>.json` for all courses, without evaluations
- `https://coursetable.com/gen/json/data_with_evals_<TERM_CODE>.json` for all courses, with evaluations

The script `fetch_previous_json.py` downloads these JSON files by term and outputs the result to the `/api_output/previous_json` directory.

## Fetching class information from the Yale APIs

We retrieve course information from two sources: the back-end used for Yale OCS, and the official Yale Courses API (https://developers.yale.edu/courses). These are performed in `fetch_classes.py`.

To retrieve courses from the Yale OCS API, we do the following:

1. Obtain a list of terms that have available course information, which is output to `/api_output/terms.json`.
2. For each term, obtain a list of the courses, which is output to `/api_output/term_courses`.
3. For each course, obtain the full course information, which is output to `/api_output/course_json_cache`.

To retrieve courses from the official Yale Courses API, we do the following:

1. Use all seasons since fall 2009 inclusive, which is the furthest the API goes to. This is output to `/api_output/api_seasons.json`.
2. For each season, obtain a list of subjects, which are required for querying courses (we can only view courses per subject per season). This is output to `/api_output/api_subjects`.
3. For each season and subject, obtain a list of courses, which we output to `/api_output/api_course_json_cache` (aggregated by season).

## Fetching evaluations

There is no API available to Yale students for querying course evaluations, but we figured out the routes that Yale's evaluations portal uses. To query the evaluations, we first have to authenticate with a Yale NetID and password, which returns a session cookie that permits access. For each term, we then query the following:

1. The questions that students respond to for rating the course
2. The numerical ratings for the course
3. The student responses to the questions