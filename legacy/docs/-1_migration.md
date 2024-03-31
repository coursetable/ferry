# Migration notes

This document explains the steps used to migrate the original (pre-June 2020) CourseTable database to the one used since. Here, we list the changes made and explain the reasons behind them.

## The previous database

Previously, the database was stored in a MySQL system. The CourseTable website interfaced with this database by generating JSON files containing all courses for each season, which were then publicly accessible. However, when viewing courses, the entire JSON file for the season had to be loaded and parsed.

## Courses

### Schema

Previously, classes were stored in a JSON file for each term. Classes were represented by entries with the following fields, summarized in the table below:

| Field                       | Type            | Description                                                                                                       | Changes                                                                                           |
| --------------------------- | --------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `areas`                     | List            | Course areas (humanities, social sciences, sciences)                                                              | Maintaining                                                                                       |
| `average`                   | Nested          | Nested object describing numerical rating averages for same professor, same class, same both averaged over terms  | **Moved ratings to a separate schema**                                                            |
| `codes`                     | List of objects | List of codes containing the class subject, number, section, and row identifier                                   | **Removed**                                                                                       |
| `course_home_url`           | String          | Link to the course homepage                                                                                       | Maintaining                                                                                       |
| `course_name_id`            | String/integer  | ?                                                                                                                 | **Removed**                                                                                       |
| `description`               | String          | Course description                                                                                                | Maintaining                                                                                       |
| `evaluations`               | Nested          | Nested object describing numerical rating averages for same professor, same class, same both for individual terms | **Moved ratings to a separate schema**                                                            |
| `exam_group`                | ?               | Zero everywhere, probably already deprecated                                                                      | **Removed**                                                                                       |
| `exam_timestamp`            | ?               | Zero everywhere, probably already deprecated                                                                      | **Removed**                                                                                       |
| `extra_info`                | String          | Additional information (indicates if class has been cancelled)                                                    | Maintaining                                                                                       |
| `flags`                     | List of strings | Detailed course areas, currently seems to be a bit broken                                                         | Maintaining                                                                                       |
| `location_times`            | Nested          | Key-value pairs consisting of `<location>:<list of times>`                                                        | **Removed**                                                                                       |
| `locations_summary`         | String          | If single location, is `<location>`; otherwise is `<location> + <n_other_locations>`                              | Maintaining                                                                                       |
| `long_title`                | String          | Complete course title                                                                                             | Renamed to `title`                                                                                |
| `num_students`              | Integer         | Student cap                                                                                                       | Maintaining                                                                                       |
| `num_students_is_same_prof` | Boolean         | Whether or not a different professor taught the class when it was this size                                       | Maintaining                                                                                       |
| `number`                    | String          | Course numbering code                                                                                             | Renamed to `course_numbering`                                                                     |
| `oci_id`                    | String          | Yale Online Course Information ID                                                                                 | Renamed to `crn`                                                                                  |
| `oci_ids`                   | List of strings | For when a course has multiple OCI IDs                                                                            | Renamed to `crns`                                                                                 |
| `professors`                | List of strings | Who teaches the course                                                                                            | Maintaining                                                                                       |
| `requirements`              | string          | Recommended requirements/prerequisites for the course                                                             | Maintaining                                                                                       |
| `row`                       | Integer         | SQL row identifier                                                                                                | **Removed**                                                                                       |
| `row_id`                    | Integer         | SQL row identifier                                                                                                | **Removed**                                                                                       |
| `section`                   | String          | Which section the course is (each section has its own field, as returned in the original API output)              | Maintaining                                                                                       |
| `skills`                    | List            | Skills that the course fulfills (e.g. writing, quantitative reasoning, language levels)                           | Maintaining                                                                                       |
| `subject`                   | String          | Course subject code                                                                                               | Maintaining                                                                                       |
| `syllabus_url`              | String          | Link to the syllabus                                                                                              | Maintaining                                                                                       |
| `times`                     | Nested          | List of times and locations that the course meets                                                                 | Exploded keys `long_summary`, `summary`, and `by_day` to individual columns prefixed with `times` |
| `title`                     | String          | Shortened course title                                                                                            | Renamed to `short_title`                                                                          |

### Other changes

- Previously, times were stored in a 24-hour float-format. For instance, "1:00 PM" would be encoded as 13.0. This was changed in the new setup to a 24-hour colon-based format.
- Rather than storing repeated fields for cross-listed courses, we designated one table in which each course was represented by a unique ID, and cross-listings were then indicated in a separate table

### Migration steps

1. Download all of the previous CourseTable JSON files with `/migration/fetch_previous_json.py`, saving to `/api_output/previous_json`
2. Download all of the previous ratings from CourseTable with `/migration/fetch_previous_ratings.py`, saving to `/api_output/previous_evals`
3. Using the new class fetching script (`/crawler/fetch_classes.py`) and parser (`/crawler/parse_classes.py`), download and preprocess recent courses from the Yale internal courses API, saving as JSONs in `/api_output/course_json_cache` and then `/api_output/parsed_courses`
4. Download and format all of the recent ratings from Yale's evaluations site with `/crawler/fetch_ratings.py`, saving to `/api_output/course_evals`
5. Format the previous CourseTable JSONs to be like the new class JSONs with `/migration/migrate_courses.py`, saving these to `/api_output/migrated_courses`
6. Run `/importer.py` to first migrate the old courses and evaluations from steps (5) and (2), and then import and overwrite with the newer outputs from (3) and (4).

## Evaluations

Evaluations were stored across separate tables, which were then put into the large JSON files for each course.
