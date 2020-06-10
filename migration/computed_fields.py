import pandas as pd

from tqdm import tqdm

from ast import literal_eval

import sys
sys.path.append("..")

from includes.utils import * 

courses_no_evals = pd.read_csv("../migrated_tables/courses_no_evals.csv",index_col=0)
courses_professors = pd.read_csv("../migrated_tables/courses_professors.csv",index_col=0)

evaluation_narratives = pd.read_csv("../migrated_tables/evaluation_narratives.csv",index_col=0)
evaluation_questions = pd.read_csv("../migrated_tables/evaluation_questions.csv",index_col=0)
evaluation_ratings = pd.read_csv("../migrated_tables/evaluation_ratings.csv",index_col=0)

listings = pd.read_csv("../migrated_tables/listings.csv",index_col=0)
professors_no_evals = pd.read_csv("../migrated_tables/professors_no_evals.csv",index_col=0)
tagged_evaluation_questions = pd.read_csv("../migrated_tables/tagged_evaluation_questions.csv",index_col=0)

print("Loaded tables")

# set ratings index for fast access
evaluation_ratings.set_index(["course_id","question_code"],inplace=True)
evaluation_ratings["ratings"] = evaluation_ratings["ratings"].apply(literal_eval)

# applied function to compute course averages and retrieve enrollment
def compute_course_averages(course_row):

    course_id = course_row.name

    try:
        course_ratings = evaluation_ratings.loc[course_id]
    
    # handle non-evaluated courses
    except KeyError:
        return None, None, None
    
    # overall ratings
    is_overall = tagged_evaluation_questions.loc[course_ratings.index, "tag"] == "rating"
    course_overall = course_ratings[is_overall]

    # workload ratings
    is_workload = tagged_evaluation_questions.loc[course_ratings.index, "tag"] == "workload"
    course_workload = course_ratings[is_workload]

    # compute averages
    average_overall, enrollment = category_average(course_overall["ratings"])
    average_workload, _ = category_average(course_workload["ratings"])

    return enrollment, average_overall, average_workload

tqdm.pandas(desc="Computing `courses` fields")
computed_averages = courses_no_evals.progress_apply(compute_course_averages, axis=1)
num_students, average_overall_rating, average_workload = zip(*computed_averages)

courses_no_evals["num_students"] = num_students
courses_no_evals["average_overall_rating"] = average_overall_rating
courses_no_evals["average_workload"] = average_workload

courses_no_evals.to_csv("../migrated_tables/courses_with_evals.csv")

print("Saved courses with computed fields")