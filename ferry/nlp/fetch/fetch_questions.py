import json

import requests

url = "http://localhost:8085/v1/graphql"


def fetch_questions():
    query = """
    query MyQuery {
        evaluation_questions(where: {is_narrative: {_eq: true}}) {
            question_code
            question_text
        }
    }
    """

    r = requests.post(url, json={"query": query}, verify=False)
    raw_data = json.loads(r.text)["data"]["evaluation_questions"]

    clean_data = {}
    for question in raw_data:
        clean_data[question["question_code"]] = question["question_text"]
    return clean_data


data = fetch_questions()
with open("./../data/questions.txt", "w+") as outfile:
    json.dump(data, outfile)
