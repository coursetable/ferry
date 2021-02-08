# Retrieval

## Previous CourseTable files

The current CourseTable implementation stores classes by season in JSON files at publicly-available URLs. To compare our new crawler against these files, we have to first download these from:

- `https://coursetable.com/gen/json/data_<SEASON_CODE>.json` for all courses without evaluations
- `https://coursetable.com/gen/json/data_with_evals_<SEASON_CODE>.json` for all courses with evaluations

The script `/ferry/migration/fetch_previous_json.py` downloads these JSON files by season and outputs the result to the `/data/previous_json/` directory.

## Fetching class information from the Yale APIs

We retrieve course information from two sources: the back-end used for Yale OCS, ~~and the official Yale Courses API (https://developers.yale.edu/courses)~~. (_Note: we have scripts to call the official courses API, but these are not in use currently because they only contain Yale College courses_). These are performed in `fetch_classes.py`.

To retrieve courses from the Yale OCS API, we do the following:

1. Obtain a list of seasons that have available course information via `/ferry/crawler/fetch_seasons.py` , which is output to `/data/course_seasons.json`.
2. For each season, obtain a list of the courses via `/ferry/crawler/fetch_classes.py`, which is output to `/data/season_courses/` per season. For each course, we then obtain the full course information, which are output to `/data/course_json_cache/` per season.

## Fetching demand statistics

As far as we know, there is no API that provides us demand statistics. To retrieve these, we use scripts based off the [yale-popular-classes](https://github.com/iamdanzhao/yale-popular-classes) repository developed by [Daniel Zhao](https://github.com/iamdanzhao) for a YDN report on popular classes. These scrape the HTML from the [course demand statistics portal](https://ivy.yale.edu/course-stats/).

Our demand statistics retrieval pipeline is as follows:

1. Obtain a list of seasons that have available demand statistics via `/ferry/crawler/fetch_seasons.py` , which is output to `/data/demand_seasons.json`.
2. Obtain a list of subject codes that have available demand statistics (the course demand statistics portal is indexed by subject code). This is performed by `/ferry/crawler/fetch_subjects.py` and is output to `/data/demand_subjects.json`.
3. For each season and subject code, get the course demand statistics for every listed class. This is performed by `/ferry/crawler/fetch_demand.py` and is output to `/data/demand_stats/` per season.

## Fetching evaluations

There is no API available to Yale students for querying course evaluations, but we figured out the routes that Yale's evaluations portal uses. To query the evaluations, we first have to authenticate with a Yale NetID and password, which returns a session cookie that permits access. For each season, we then query the following:

1. The questions that students respond to for rating the course
2. The numerical ratings for the course
3. The student responses to the questions

The evaluations crawler is implemented in `/ferry/crawler/fetch_ratings.py`. The seasons to crawl are specified manually. For each season, we crawl courses based on the CRN values from lists in`/data/season_courses/`. The evaluations are then output per CRN to `/data/course_evals/`. Note that we also output the raw HTML to `/data/rating_cache/` for debugging purposes.

Note that evaluations typically only go back a couple of years. For earlier evaluation info since 2011, we migrated those from the previous CourseTable site (see `/ferry/migration/fetch_previous_ratings.py`).
