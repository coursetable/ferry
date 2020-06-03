# Parsing

## Changes to the old schema

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

## Schemas

### Seasons: `seasons`

| Field         | Type                                       | Description            |
| ------------- | ------------------------------------------ | ---------------------- |
| `season_id`   | Identifier                                 | Season id              |
| `season_code` | String (e.g. "202001")                     | The season code        |
| `season`      | String - one of `spring`, `summer`, `fall` | Season of the semester |
| `year`        | Integer                                    | Year of the semester   |

### Courses: `courses`

One entry per class. If a class is listed with multiple course codes, it will only get one entry in this database.

| Field                       | Type            | Description                                                  |
| --------------------------- | --------------- | ------------------------------------------------------------ |
| `course_id`                 | Identifier      | Course id                                                    |
| `season`                    | Foreign Key     | The season that the course is being taught in, mapping to `seasons` |
| `areas`                     | List            | Course areas (humanities, social sciences, sciences)         |
| `course_home_url`           | String          | Link to the course homepage                                  |
| `description`               | String          | Course description                                           |
| `extra_info`                | String          | Additional information (indicates if class has been canceled) |
| `flags`                     | List of strings | Detailed course areas, currently seems to be a bit broken    |
| `location_times`            | Nested          | Key-value pairs consisting of `<location>:<list of times>`   |
| `locations_summary`         | String          | If single location, is `<location>`; otherwise is `<location> + <n_other_locations>` where the first location is the one with the greatest number of days |
| `num_students`              | Integer         | Student cap                                                  |
| `num_students_is_same_prof` | Boolean         | Whether or not a different professor taught the class when it was this size |
| `requirements`              | String          | Recommended requirements/prerequisites for the course        |
| `section`                   | String          | Which section the course is (each section has its own field, as returned in the original API output) |
| `sessions`                  | Nested          | List of dictionaries, each of which specifies meeting at a specific location for a specific time period `'days':[list_of_days]`,`'start_time':<start_time>`, `'end_time':<end_time>`, `'location':<location>` |
| `short_title`               | String          | Shortened course title                                       |
| `skills`                    | List            | Skills that the course fulfills (e.g. writing, quantitative reasoning, language levels) |
| `syllabus_url`              | String          | Link to the syllabus                                         |
| `times`                     | Nested          | List of times and locations that the course meets            |
| `title`                     | String          | Complete course title                                        |
| `average_overall_rating`    | Float           | [computed] Average overall course rating (from this course's evaluations, aggregated across cross-listings) |
| `average_workload`          | Float           | [computed] Average workload rating ((from this course's evaluations, aggregated across cross-listings) |

### Listings: `listings`

Each course code (e.g. "AMST 312") and season will get one entry in this database.

| Field         | Type        | Description                                      |
| ------------- | ----------- | ------------------------------------------------ |
| `listing_id`  | Identifier  | Listing ID                                       |
| `course_id`   | Foreign Key | Course that the listing refers to                |
| `subject`     | String      | Subject the course is listed under (e.g. "AMST") |
| `number`      | String      | Course number in the given subject               |
| `course_code` | String      | [computed] subject + number (e.g. "AMST 312")    |
| `section `    | String      | Course section for the given subject and number  |

### Professors: `professors`

| Field            | Type       | Description                                                  |
| ---------------- | ---------- | ------------------------------------------------------------ |
| `professor_id`   | Identifier | Professor ID                                                 |
| `average_rating` | Float      | [computed] Average rating of the professor assessed via the "Overall assessment" question in courses taught |
| `name`           | String     | Name of the professor                                        |

### Course-Professor Junction Table `courses_professors`

| Field          | Type        | Description  |
| -------------- | ----------- | ------------ |
| `course_id`    | Foreign Key | Course ID    |
| `professor_id` | Foreign Key | Professor ID |

### Historical Ratings `historical_ratings`

| Field                     | Type                   | Description  |
| ------------------------- | ---------------------- | ------------ |
| `course_code`             | String (e.g. CPSC 366) | Course ID    |
| `professor_id`            | Foreign Key            | Professor ID |
| `course_rating_all_profs` | Float                  | [computed]   |
| `course_rating_this_prof` | Float                  | [computed]   |
| `course_workload`         | Float                  | [computed]   |

### Evaluations (questions): `evaluation_questions`

| Field           | Type                  | Description                                              |
| --------------- | --------------------- | -------------------------------------------------------- |
| `question_id`   | Identifier            | Question ID                                              |
| `question_code` | String (e.g. "YC402") | Question code (from OCE)                                 |
| `is_narrative`  | Bool                  | True if the question has narrative responses             |
| `question_text` | String                | The question                                             |
| `options`       | List of strings       | Possible responses (only if the question is categorical) |

### Evaluations (narrative): `evaluation_narratives`

| Field            | Type        | Description                                                  |
| ---------------- | ----------- | ------------------------------------------------------------ |
| `course_id`      | Foreign Key | Course the narrative comment applies to, mapping to `courses` |
| `question_id`    | Foreign Key | Question the answer is a response to, mapping to `evaluation_questions` |
| `comment`        | String      | Response to the question                                     |
| `comment_length` | Integer     | [computed] Length of the response in characters              |

### Evaluations (ratings): `evaluation_ratings`

| Field             | Type             | Description                                                  |
| ----------------- | ---------------- | ------------------------------------------------------------ |
| `course_id`      | Foreign Key | Course the narrative comment applies to, mapping to `courses` |
| `question_id`    | Foreign Key | Question the answer is a response to, mapping to `evaluation_questions` |
| `ratings` | List of integers | Number of responses for each option. The options are listed in the `evaluation_questions` table |