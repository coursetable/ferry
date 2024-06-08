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

## Same courses

Finding identical offerings of a class over time is essential to the following features:

1. The historical mean rating and workload for a class, calculated over all past offerings of the same class.
2. The most recent offering of a class, and its associated attributes (enrollment in particular).
3. Any statistical analysis on how courses may have changed over time.

It is one of our key algorithms that we spend the most time on.

### Goals

Our course matching algorithm should simultaneously avoid false-positives and false-negatives. While both are inevitable because the degree to which a class can change is on a continuum, in general we try to err on the generous side and prefer false-positives to false-negatives (that is, consider more courses to be the same course).

In particular, here are a few methods that _don't work_:

- For each course, list all courses with the same code as the same course. This creates too many false-positives, because course codes may be recycled. For example, CPSC 427 had been "Object-Oriented Programming" up to fall 2019, but changed to CPSC 327 in fall 2020; in fall 2024, CPSC 427 re-appears with the title "C++ Programming for Stability, Security, and Speed". We don't want to consider the two courses called CPSC 427 as the same course and link their ratings.
- For each course, list all courses with the same title as the same course. This creates too many false-negatives, because course titles may change, such as changing capitalization, adding/removing words, etc. This may also create false-positives, for example in fall 2020, two courses both called "Foundations of Accounting and Valuation" coexisted in the catalog, but with completely different schools, professors, and course codes (ACCT 270 vs. MGT 502/HPM 502).
- For each course, _only_ consider courses with the same code and similar title/description. This is generally fine! We can still successfully link two courses with unrelated codes together, as long as they use a "bridge" that cross-list both (because we use a connected-components model; see below). However, consider the following hypothetical case: in season 1, course X has code A; in season 2, it has code A and B; in season 3, it has code B. It turns out that seasons 1 and 3 offered the same content but season 2 was different. We will fail to link them because the bridge doesn't exist. This may happen with seminars, where every season a different set of topics are offered.

Therefore, we use a combination of course code, title, and description to match courses.

### Methodology

We reformulate this task as computing a partition of our set of courses into groups of related courses. Each group contains different offerings of the same course, and our set of groups is pairwise disjoint. Then, we just assign a unique group label to each course.

> [!NOTE]
> We are considering whether this partition-based model fits our needs. Sometimes a course X may split into two courses Y and Z, and it seems more appropriate to link X to both but not link Y with Z.

Note that the same-course relationship does not amount to an equivalence relationship – it's possible for courses _A_, _B_ as well as _B_, _C_ to be similar but _A_ and _C_ to have below-threshold similarity which violates the transitivity condition. This pattern occurs when course content drifts over time.

To find these groups of related courses, we model the set of courses as nodes in a graph. (Discussion sections don't participate in this graph.) We then connect the nodes and find the [connected components](<https://en.wikipedia.org/wiki/Component_(graph_theory)>) in this graph. For performance reasons, we make several optimizations, whereby we use heuristics to group courses before doing exhaustive pairwise comparisons.

First, we group the courses by overlapping course codes. We consider courses that carry course codes that have ever co-existed to be potentially related. For example, SPAN 267 "Studies in Latin American Literature II" cross-listed with LITR 258 in spring 2021, while LITR 258 cross-listed with ENGL 297 "Literature on Migration in Asian America and East Asia" in fall 2009, and so on, causing all of these codes to be grouped together. We have to manually merge some groups in case courses change codes without any hints of cross-listings; for example, CPSC 427 changed to CPSC 327 but they had never been cross-listed. The course-code grouping is very generous and defines the upper bound of same-course groups.

Then, we split each course-code group by their titles. Courses with the same title in each group are always considered the same. The title grouping is very strict and defines the lower bound of the same-course groups.

Finally, we try to find a good middle point between the title groups and the course-code groups by connecting as many title groups as possible. We rely on the normalized text distance, which is defined as the [Levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance) between two strings, with infix alignment (that is, if A has extra characters on both ends compared to B, those characters are removed for free), divided by the length of the shorter string. Two title groups are connected if:

- they have titles with distance below a threshold, OR
- there exists a course in each group, such that their descriptions have distance below a threshold.

We also define some manual overrides to prevent similar titles being considered equal if they are actually different.

After figuring out the history of a course, we can calculate the average rating and workload of the course over time. This is done by taking the average of the ratings and workloads of the course in each season.

## Professor average rating

Each professor also has an average rating, which is done by taking the simple average of the average rating of all courses with this professor listed as instructor. We have since known many issues with this approach:

- Some instructors are listed for a course purely for administrative reasons, especially for senior projects. (https://github.com/coursetable/coursetable/issues/1601)
- The average does not represent development; old courses may continue to penalize the average even with improvement.
- The average may be skewed by intro classes which tend to have lower ratings.
- The average may be skewed by small classes where there are only one or two extreme ratings. Note that this point is in contradiction with the previous one, though: if we implement weights based on number of ratings, big intro classes will penalize the average even further.

We are exploring ways to improve this calculation in https://github.com/coursetable/coursetable/issues/1602.

Each course also has an `average_professor_rating`, which is calculated by taking the simple average of the professor average rating for all professors associated with this course.
