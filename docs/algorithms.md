# Algorithms

Below is the documentation for how Ferry calculates some computed attributes.

## Cross-listing matching

Ferry makes the distinction between _courses_ and _listings_.

- A _listing_ is a unique registration entity. It is identified by either: season + CRN, or season + course code + section number.
- A _course_ is a unique real-life entity. It may be identified by the time and location (that is, if you ask "what's happening in YSB MARSH at Monday 9am?" the answer would be a course), or title + section, or syllabus, etc., but no really good real-life-relevant key exists. Usually, you identify a course by first identifying one listing (such as Spring 2024, CPSC 365, section 1) and then finding its related course.

A course can correspond to multiple listings (in the case of cross-listings), but a listing can only correspond to one course. Check the [DB diagram](./db_diagram.pdf) for what attributes courses and listings carry. In general, anything "interesting" is on the course. Only registration-specific information: school, course code (subject + number), and CRN are unique to listings. The listing's section and season code should all be equal to those of its course's and are only here for searching purposes.

Our cross-listing matching entirely relies on the YCS-provided information. On YCS, there's a "Same As" row. We parse the CRN of this list of cross listings. The CRN graph should be a disjoint union of cliques—that is, if in a season, A only cross-lists B, then B should only cross-list A, and no other listing should cross-list A or B. We assign each such clique of listings a `course_id`. Finally, we deduplicate the listings by the `course_id`. There's no check that the cross-listings actually contain the same information. We will pick the listing that (a) is offered by Yale College (b) has the smallest CRN.

## Professor identification

On YCS, professors are listed with their name and email. Additionally there's an ID field, but we have learned that YCS recycles the ID so we decided to ignore this ID. Therefore, we attempt to aggregate professor information using name and email only. We do it in the following way:

- First, we attempt to fill empty emails by finding entries with the same professor name.
  - If there are multiple emails corresponding to the same name, there is no guaranteed order. For example: if there are three name-email pairs: (A, ""), (A, a), (A, b), we will fill the first email with either a or b since there's no better way for us to know.
- Then, we have some name-email pairs and some name-only pairs (whose names don't overlap with those with emails). The name-only pairs are identified by name. The name-email pairs are identified by email. This means:
  - If a professor changes their registered name, and all emails are empty, they will be treated as separate professors.
  - If there are multiple names corresponding to the same email, we see this as a registered name change. The most recent name is kept.
  - If there are multiple emails corresponding to the same name, they are treated as separate professors. Usually this means they are two people with the same name.

## Course average rating & workload

Each course is associated with one average rating and one average workload, accessible via GraphQL `courses#evaluation_statistics#{avg_rating,avg_workload}`. These are calculated as follows:

- First, we find the evaluation questions that pertain to overall rating and workload rating. All questions tagged with `Overall` or `Workload` will be used for calculation. Tag assignment is almost purely keyword-based and is available via GraphQL `evaluation_questions#tag`.
- Each course can have several questions tagged with `Overall` or `Workload`. This usually comes from the course having both a YC and a GS cross-listing. Each question should have 5 options each corresponding to its number of responses. We sum the response numbers across all questions.
- Finally, we take the average: with 1, 2, 3, 4, 5 as the scores and the number of responses as the weights.

## Course history

TODO

After figuring out the history of a course, we can calculate the average rating and workload of the course over time. This is done by taking the average of the ratings and workloads of the course in each season.

## Professor average rating

Each professor also has an average rating, which is done by taking the simple average of the average rating of all courses with this professor listed as instructor. We have since known many issues with this approach:

- Some instructors are listed for a course purely for administrative reasons, especially for senior projects. (https://github.com/coursetable/coursetable/issues/1601)
- The average does not represent development; old courses may continue to penalize the average even with improvement.
- The average may be skewed by intro classes which tend to have lower ratings.
- The average may be skewed by small classes where there are only one or two extreme ratings. Note that this point is in contradiction with the previous one, though: if we implement weights based on number of ratings, big intro classes will penalize the average even further.

We are exploring ways to improve this calculation in https://github.com/coursetable/coursetable/issues/1602.

Each course also has an `average_professor_rating`, which is calculated by taking the simple average of the professor average rating for all professors associated with this course.
