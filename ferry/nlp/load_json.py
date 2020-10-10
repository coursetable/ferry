from collections import defaultdict
from os import listdir
import json

"""
with open("data/evals/201903.txt", "r+") as infile:
    current_courses = json.load(infile)

with open("data/questions.txt", "r+") as infile:
    questions = json.load(infile)
"""

"""
def get_course(course_num):
    course_data = current_courses[course_num]
    course_id = course_data["course_id"]
    title = course_data["title"]
    # season_code = data["season_code"]
    professors = [x["professor"]["name"] for x in course_data["course_professors"]]
    listings = course_data["listings"]
    avg_rating = course_data["average_rating"]
    avg_workload = course_data["average_workload"]
    description = course_data["description"]
    evaluation_narratives = defaultdict(list)
    for eval in course_data["evaluation_narratives"]:
        evaluation_narratives[eval["question_code"]].append(eval["comment"])

    return {
        "course_id": course_id,
        "title": title,
        "listings": listings,
        "professors": professors,
        "description": description,
        "avg_rating": avg_rating,
        "avg_workload": avg_workload,
        "evaluations": evaluation_narratives,
    }
"""


def get_term_courses(term):
    with open("./data/evals/" + term) as infile:
        courses = json.load(infile)
    out = []

    for course in courses:
        subject = course["listings"][0]["subject"]
        number = course["listings"][0]["number"]
        section = course["listings"][0]["section"]

        title = course["title"]
        description = course["description"]

        enrollment = round(len(course["evaluation_narratives"]) / 3)

        listings = course["listings"]
        skills = course["skills"]
        areas = course["areas"]

        evaluations = defaultdict(list)
        for eval in course["evaluation_narratives"]:
            evaluations[eval["question_code"]].append(eval["comment"])

        if section == "1" and int(number[:3]) < 470 and "L" not in "".join(skills):
            out.append(
                [
                    [subject, number, title],
                    description,
                    enrollment,
                    listings,
                    skills,
                    areas,
                    evaluations,
                ]
            )

    return out


def get_all_courses():
    out = []
    for f in listdir("./data/evals"):
        out.extend(get_term_courses(f))
    return out


def get_current_courses():
    return get_term_courses("201903.txt")


def get_columns(courses):
    titles = [x[0] for x in courses]
    descriptions = [x[1] for x in courses]
    enrollments = [x[2] for x in courses]
    listings = [x[3] for x in courses]
    skills = [x[4] for x in courses]
    areas = [x[5] for x in courses]
    evaluations = [x[6] for x in courses]

    return {
        "titles": titles,
        "descriptions": descriptions,
        "enrollments": enrollments,
        "listings": listings,
        "skills": skills,
        "areas": areas,
        "evaluations": evaluations,
    }
