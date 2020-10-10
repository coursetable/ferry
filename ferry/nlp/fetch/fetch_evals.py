import json

import requests

url = "http://localhost:8085/v1/graphql"


def fetch_evals(term, dump=True):
    query = (
        """
        query MyQuery {
            courses(where: {
                season: {season_code: {_eq: \""""
        + str(term)
        + """\"}},
                school: {_eq: "YC"},
                average_rating: {_is_null: false},
                average_workload: {_is_null: false},
                extra_info: {_neq: "CANCELLED"}
            }) {
                course_id
                title
                season_code
                course_professors {professor {name}}
                listings {subject number section}
                average_rating
                average_workload
                description
                evaluation_narratives {question_code comment}
                evaluation_statistics {enrollment}
                skills
                areas
            }
        }
        """
    )

    r = requests.post(url, json={"query": query}, verify=False)
    data = json.loads(r.text)["data"]["courses"]

    if dump:
        with open("./../data/evals/" + str(term) + ".txt", "w+") as outfile:
            json.dump(data, outfile)
    return data


for year in range(2009, 2021):
    print(year)
    for term in range(1, 4):
        fetch_evals(str(year) + "0" + str(term), True)
