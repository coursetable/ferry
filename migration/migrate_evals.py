import ujson

from os import listdir
from os.path import isfile, join

from tqdm import tqdm

import pandas as pd

"""
================================================================
This script uses the previous CourseTable JSON files and
parsed Yale API files to continue migration of the existing
CourseTable data to the new schema. 

In particular, this script outputs CSV files for the following
tables specified in the schema (docs/2_parsing.md):
    
    - `evaluation_questions`
    - `evaluation_narratives`
    - `evaluation_ratings`

================================================================
"""

# load listings
listings = pd.read_csv("../migrated_tables/listings.csv", index_col=0)

# map crns to unqiue course IDs for cross-listings
listing_to_course_id = dict(zip(listings.index, listings["course_id"]))

listed_ids = set(listings.index)

# load evaluations
evaluations_path = "../api_output/previous_evals/"

# get evaluation JSONs
evaluation_jsons = [f for f in listdir(
    evaluations_path) if isfile(join(evaluations_path, f))]
evaluation_jsons = [f for f in evaluation_jsons if f[-5:] == ".json"]

all_evaluations = []

pbar = tqdm(total=len(evaluation_jsons))
pbar.set_description(f"Loading evaluation JSONs")

for json_path in evaluation_jsons:
    with open(evaluations_path+json_path, "r") as f:
        all_evaluations.append(ujson.load(f))

    pbar.update(1)

pbar.close()

for evaluation in all_evaluations:
    evaluation["listing_id"] = f"{str(evaluation['crn_code'])}_{str(evaluation['season'])}"

# get rid of evaluations without a matching course
all_evaluations = [x for x in all_evaluations if x["listing_id"] in listed_ids]

# tuples of (question_code, is_narrative, question_text, options)
evaluation_questions = []
# tuples of (course_id, question_code, comment, comment_length)
evaluation_narratives = []
# tuples of (course_id, question_code, ratings)
evaluation_ratings = []

pbar = tqdm(total=len(all_evaluations))
pbar.set_description(f"Converting evaluations to tables")

for evaluation in all_evaluations:

    course_id = listing_to_course_id[evaluation["listing_id"]]

    for q in evaluation["narratives"]:

        question_row = [
            q["question_id"],
            True,
            q["question_text"],
            []
        ]

        evaluation_questions.append(question_row)

        for c in q["comments"]:

            comment_row = [
                course_id,
                q["question_id"],
                c,
                len(c)
            ]

            evaluation_narratives.append(comment_row)

    for q in evaluation["ratings"]:

        question_row = [
            q["question_id"],
            False,
            q["question_text"],
            q["options"]
        ]

        evaluation_questions.append(question_row)

        ratings_row = [
            course_id,
            q["question_id"],
            q["data"]
        ]

        evaluation_ratings.append(ratings_row)

    pbar.update(1)

pbar.close()

# make evaluation_questions table
evaluation_questions = pd.DataFrame(evaluation_questions,
                                    columns=[
                                        "question_code",
                                        "is_narrative",
                                        "question_text",
                                        "options"
                                    ]
                                    )

evaluation_questions.drop_duplicates(
    subset=["question_code"],
    keep="first",
    inplace=True
)
evaluation_questions.set_index("question_code", inplace=True)
evaluation_questions.to_csv("../migrated_tables/evaluation_questions.csv")
print("Saved `evaluation_questions` table")

# make evaluation_narratives table
evaluation_narratives = pd.DataFrame(evaluation_narratives,
                                     columns=[
                                         "course_id",
                                         "question_code",
                                         "comment",
                                         "comment_length"
                                     ]
                                     )

# remove cross-listing duplicates
evaluation_narratives.drop_duplicates(
    subset=[
        "course_id",
        "question_code",
        "comment"
    ],
    keep="first",
    inplace=True
)
evaluation_narratives.reset_index(drop=True, inplace=True)
evaluation_narratives.to_csv("../migrated_tables/evaluation_narratives.csv")
print("Saved `evaluation_narratives` table")

# make evaluation_ratings table
evaluation_ratings = pd.DataFrame(evaluation_ratings,
                                  columns=[
                                      "course_id",
                                      "question_code",
                                      "ratings"
                                  ]
                                  )

# remove cross-listing duplicates
evaluation_ratings.drop_duplicates(
    subset=[
        "course_id",
        "question_code"
    ],
    keep="first",
    inplace=True
)
evaluation_ratings.reset_index(drop=True, inplace=True)
evaluation_ratings.to_csv("../migrated_tables/evaluation_ratings.csv")
print("Saved `evaluation_ratings` table")
