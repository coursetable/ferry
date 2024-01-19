# Matching same classes

A major part of CourseTable's appeal is our ability to provide historical information about classes, for instance the ratings the last time a class was offered. In fact, finding identical offerings of a class over time is essential to the following features:

1. The historical mean rating and workload for a class, calculated over all past offerings of the same class. Also calculate this value for the same class taught with the same set of professors.
2. The most recent offering of a class, and its associated attributes (enrollment in particular).
3. Any statistical analysis on how courses may have changed over time.

## Considerations

Initially, we used the **codes** to match classes with offerings in other semesters. For instance, in fall of 2019 and 2020, "Discrete Mathematics" was cross-listed as **AMTH 244** and **MATH 244**. Therefore, our strategy for identifying the other offerings of a course was to simply take its codes and find all classes that have at least one code in common.

However, we later noticed that often different classes [reused a same code](https://docs.google.com/document/d/1CjAwgnbwTu-BATFX6o4qDYUcaQNo4JQ_V36h6qL8hy0/edit#), which introduced many false positives. We started considering additional attributes, such as course titles and descriptions.

Using course title alone would have not been enough, however. For instance, courses sometimes change their titles over the years, which would lead to our method failing to detect these. In addition, different courses are often offered under the same title even in the same semester. For instance, in Fall of 2020, Rick Antle taught "Foundations of Accounting and Valuation" as a Yale College course under **ACCT 270**, but Anya Nakhmurina taught a different course under the same name at the school of management, under the codes **MGT 502** and **HPM 502**.

Using course description also introduces similar caveats as course titles do. In fact, professors are likely to make small modifications to their descriptions over time (for instance, changing the textbook used) that make exact matching suboptimal. Although we can use measures of fuzzy string similarity as a heuristic, we would ideally want to consider all three of code, title, and description when making our judgments.

## Methodology

Our input data source is a table of classes as well as one of listings – the `courses` and `listings` tables. The `courses` table stores a unique entry for each offering of a course, deduplicated by cross-listings (using cross-listing data provided by Yale), along with several attributes including title and description. The `listings` table stores the cross-listings of each course, which amounts to the code a course was listed in, as well as a pointer back to the deduplicated course in `courses`.

The stated objective is to find all equivalent offerings for a given course. Since this amounts to evaluating an equivalence relationship on our set of courses, we can reformulate this task as computing a partition of our set of courses into groups of equivalent courses. Each group contains different offerings of the same course, and our set of groups is pairwise disjoint. Therefore, we just assign a unique group label to each course.

Note that our previous strategy of identifying equivalent courses by shared code does not amount to an equivalence relationship – it's possible for courses _A_, _B_ as well as _B_, _C_ to have common codes but _A_ and _C_ to have no common ones, which violates the transitivity condition. This pattern sometimes occurs when course codes drift over time.

To find these groups of equivalent courses, or equivalence classes, we model the set of courses as nodes in a graph, where each course is represented by a node. We then connect two nodes if and only if they are the same course. As a result, finding groups of equivalent courses amounts to finding [connected components](<https://en.wikipedia.org/wiki/Component_(graph_theory)>) in this graph, and it is easy to see that connected components are themselves a partition of the set of courses as desired. In fact, we previously used connected components when resolving cross-listings (sometimes Yale's cross-listings are incomplete).

Therefore, our methodology is as follows:

1. For each course in `courses`, create a node in the graph corresponding to the course.
2. Connect the pairs of courses that share at least one code in common.
3. Prune the edges to remove false positives. Given an edge between two courses _A_ and _B_, keep it only if at least one of the following applies:
   - A and B have the exact same title or description.
   - The normalized text distance between the titles is below some threshold.
   - The normalized text distance between the descriptions is below some threshold.
   - Both titles and both descriptions are blank (benefit of the doubt, this is almost never the case).
4. Across our filtered graph, extract the connected components.
5. Assign each course the ID of its connected component as our desired partition ID.
6. Further split these connected component partitions by groups of same-professors for calculating same-course same-professor values.
