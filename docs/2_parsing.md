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

### Semesters: `semesters`

| Field      | Type                              | Description                                              |
| ---------- | --------------------------------- | -------------------------------------------------------- |
| `courses`  | List of identifiers               | Courses taught during the semester, mapping to `courses` |
| `season`   | One of `spring`, `summer`, `fall` | Season of the semester                                   |
| `semester` | String                            | The semester                                             |
| `year`     | Integer                           | Year of the semester                                     |

### Courses: `courses`



| Field                       | Type                | Description                                                  |
| --------------------------- | ------------------- | ------------------------------------------------------------ |
| `areas`                     | List                | Course areas (humanities, social sciences, sciences)         |
| `course_home_url`           | String              | Link to the course homepage                                  |
| `course_numbering`          | String              | Course numbering code                                        |
| `description`               | String              | Course description                                           |
| `evaluation_questions`      | List of identifiers | Short-answer evaluation questions for the course, mapping to `evaluation_questions` |
| `evaluation_ratings`        | List of identifiers | Categorical ratings for the course, mapping to `evaluation_ratings` |
| `extra_info`                | String              | Additional information (indicates if class has been cancelled) |
| `flags`                     | List of strings     | Detailed course areas, currently seems to be a bit broken    |
| `location_times`            | Nested              | Key-value pairs consisting of `<location>:<list of times>`   |
| `locations_summary`         | String              | If single location, is `<location>`; otherwise is `<location> + <n_other_locations>` |
| `num_students`              | Integer             | Student cap                                                  |
| `num_students_is_same_prof` | Boolean             | Whether or not a different professor taught the class when it was this size |
| `oci_ids`                   | List of strings     | For when a course has multiple OCI IDs                       |
| `professors`                | List of identifiers | Who teaches the course, mapping to `professors`              |
| `requirements`              | string              | Recommended requirements/prerequisites for the course        |
| `section`                   | String              | Which section the course is (each section has its own field, as returned in the original API output) |
| `semester`                  | Identifier          | The semester that the course is being taught in, mapping to `semesters` |
| `short_title`               | String              | Shortened course title                                       |
| `skills`                    | List                | Skills that the course fulfills (e.g. writing, quantitative reasoning, language levels) |
| `subject`                   | String              | Course subject code                                          |
| `syllabus_url`              | String              | Link to the syllabus                                         |
| `times`                     | Nested              | List of times and locations that the course meets            |
| `title`                     | String              | Complete course title                                        |

### Professors: `professors`

| Field            | Type                | Description                                                  |
| ---------------- | ------------------- | ------------------------------------------------------------ |
| `average_rating` | Float               | Average rating of the professor assessed via the "Overall assessment" question in courses taught |
| `courses`        | List of identifiers | List of courses in `courses` that the professor has taught/is teaching |
| `name`           | String              | Name of the professor                                        |

### Evaluations (short-answer questions): `evaluation_questions`

| Field           | Type                | Description                                             |
| --------------- | ------------------- | ------------------------------------------------------- |
| `course`        | Identifier          | Course the question was asked for, mapping to `courses` |
| `question_text` | String              | The question                                            |
| `responses`     | List of identifiers | Responses to the question                               |

### Evaluations (short-answer comments): `evaluation_comments`

| Field            | Type       | Description                                                  |
| ---------------- | ---------- | ------------------------------------------------------------ |
| `comment`        | String     | Response to the question                                     |
| `comment_length` | Integer    | Length of the response in characters                         |
| `question`       | Identifier | Question the answer is a response to, mapping to `evaluation_comments` |

### Evaluations (categorical ratings): `evaluation_ratings`

| Field             | Type             | Description                                                  |
| ----------------- | ---------------- | ------------------------------------------------------------ |
| `categories`      | List of strings  | Sorted list of options for the question                      |
| `category_counts` | List of integers | Number of responses to each corresponding option in `categories` |
| `course`          | Identifier       | Course the evaluation is corresponding to                    |