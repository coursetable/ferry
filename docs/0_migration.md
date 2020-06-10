# Migration notes

This document explains the steps used to migrate the original (pre-June 2020) CourseTable database to the one used since. Here, we list the changes made and explain the reasons behind them.

## The previous database

Previously, the database was stored in a MySQL system. The CourseTable website interfaced with this database by generating JSON files containing all courses for each season, which were then publicly accessible. However, when viewing courses, the entire JSON file for the season had to be loaded and parsed, which was 

## Courses

Previously, classes were stored in a JSON file for each term. This table summarizes the changes to be made:

| Field                       | Type            | Description                                                  | Changes                                 |
| --------------------------- | --------------- | ------------------------------------------------------------ | --------------------------------------- |
| `areas`                     | List            | Course areas (humanities, social sciences, sciences)         | Maintaining                             |
| `average`                   | Nested          | Nested object describing numerical rating averages for same professor, same class, same both averaged over terms | **Moving ratings to a separate schema** |
| `codes`                     | List of objects | List of codes containing the class subject, number, section, and row identifier | **Removing**                            |
| `course_home_url`           | String          | Link to the course homepage                                  | Maintaining                             |
| `course_name_id`            | String/integer  | ?                                                            | **Removing**                            |
| `description`               | String          | Course description                                           | Maintaining                             |
| `evaluations`               | Nested          | Nested object describing numerical rating averages for same professor, same class, same both for individual terms | **Moving ratings to a separate schema** |
| `exam_group`                | ?               | Zero everywhere, probably already deprecated                 | **Removing**                            |
| `exam_timestamp`            | ?               | Zero everywhere, probably already deprecated                 | **Removing**                            |
| `extra_info`                | String          | Additional information (indicates if class has been cancelled) | Maintaining                             |
| `flags`                     | List of strings | Detailed course areas, currently seems to be a bit broken    | Maintaining                             |
| `location_times`            | Nested          | Key-value pairs consisting of `<location>:<list of times>`   | Maintaining                             |
| `locations_summary`         | String          | If single location, is `<location>`; otherwise is `<location> + <n_other_locations>` | Maintaining                             |
| `long_title`                | String          | Complete course title                                        | Renaming to `title`                     |
| `num_students`              | Integer         | Student cap                                                  | Maintaining                             |
| `num_students_is_same_prof` | Boolean         | Whether or not a different professor taught the class when it was this size | Maintaining                             |
| `number`                    | String          | Course numbering code                                        | Renaming to `course_numbering`          |
| `oci_id`                    | String          | Yale Online Course Information ID                            | **Removing**                            |
| `oci_ids`                   | List of strings | For when a course has multiple OCI IDs                       | Maintaining                             |
| `professors`                | List of strings | Who teaches the course                                       | Maintaining                             |
| `requirements`              | string          | Recommended requirements/prerequisites for the course        | Maintaining                             |
| `row`                       | Integer         | SQL row identifier                                           | **Removing**                            |
| `row_id`                    | Integer         | SQL row identifier                                           | **Removing**                            |
| `section`                   | String          | Which section the course is (each section has its own field, as returned in the original API output) | Maintaining                             |
| `skills`                    | List            | Skills that the course fulfills (e.g. writing, quantitative reasoning, language levels) | Maintaining                             |
| `subject`                   | String          | Course subject code                                          | Maintaining                             |
| `syllabus_url`              | String          | Link to the syllabus                                         | Maintaining                             |
| `times`                     | Nested          | List of times and locations that the course meets            | Maintaining                             |
| `title`                     | String          | Shortened course title                                       | Renaming to `short_title`               |

The main changes to implement are as follows:

- Replace the current bulk JSON-based loader with a proper API for querying course info
- Move evaluations to a separate collection/table
- Migrate the existing data to this new scheme (because Yale's API appears to be limited to the past five years)

7

## Evaluations

