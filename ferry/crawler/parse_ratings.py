from pathlib import Path

import ujson
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from ferry import config
from ferry.includes.tqdm import tqdm

analyzer = SentimentIntensityAnalyzer()


def get_sentiment(comment):
    sentiment = analyzer.polarity_scores(comment)
    sentiment["comment"] = comment

    return sentiment


def process_narratives(narratives):
    processed_narratives = []

    for narrative in narratives:
        processed_narrative = {}
        processed_narrative["question_text"] = narrative["question_text"]
        processed_narrative["question_id"] = narrative["question_id"]
        processed_narrative["comments"] = [
            get_sentiment(comment) for comment in narrative["comments"]
        ]

        processed_narratives.append(processed_narrative)

    return processed_narratives


# list available evaluation files
previous_eval_files = Path(config.DATA_DIR / "previous_evals").glob("*.json")
new_eval_files = Path(config.DATA_DIR / "course_evals").glob("*.json")

previous_eval_files = [x.name for x in previous_eval_files]
new_eval_files = [x.name for x in new_eval_files]

all_evals = sorted(list(set(previous_eval_files + new_eval_files)))

merged_evaluations = []

for filename in tqdm(all_evals, desc="Loading evaluation JSONs"):
    # Read the evaluation, giving preference to current over previous.
    current_evals_file = Path(f"{config.DATA_DIR}/course_evals/{filename}")

    if current_evals_file.is_file():
        with open(current_evals_file, "r") as f:
            evaluation = ujson.load(f)
    else:
        with open(f"{config.DATA_DIR}/previous_evals/{filename}", "r") as f:
            evaluation = ujson.load(f)

    merged_evaluations.append(evaluation)


for evaluation in tqdm(merged_evaluations, desc="Processing evaluations"):
    evaluation["narratives"] = process_narratives(evaluation["narratives"])

with open(f"{config.DATA_DIR}/merged_evaluations.json", "w") as f:
    f.write(ujson.dumps(merged_evaluations, indent=4))
