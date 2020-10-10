from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import matplotlib.pyplot as plt

from load_json import get_all_courses, get_current_courses, get_columns

data = get_current_courses()
columns = get_columns(data)
titles = [" ".join(x) for x in columns["titles"]]
evaluations = columns["evaluations"]
N = len(titles)

# "YC401": "What knowledge, skills, and insights did you develop by taking this course?"
# "YC403": "What are the strengths and weaknesses of this course and how could it be improved?"
# "YC409": "Would you recommend this course to another student? Please explain."

analyzer = SentimentIntensityAnalyzer()
cutoffs = [-1, -0.5, 0, 0.5, 0.75, 1]

courses = {}
for i in range(N):
    zones = [0, 0, 0, 0, 0]  # very bad, bad, neutral, good, very good
    texts = evaluations[i]["YC409"]
    scores = {}

    for j, text in enumerate(texts):
        if len(text) < 20:
            continue

        score = analyzer.polarity_scores(text)["compound"]
        for k in range(5):
            if cutoffs[k] < score < cutoffs[k + 1]:
                zones[k] += 1

        if len(text) > 100:
            scores[j] = score

    count = sum(zones)
    if count > 5:
        print(titles[i])
        average = sum([zones[i] * i * 1 / 4 for i in range(5)]) / count
        print(str(round(100 * average)) + "% Positive Ratings")
        print("Rating Breakdown: " + str(zones) + "\n")
        courses[titles[i]] = average

        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        if sorted_scores[0][1] > 0.5:
            print("Sample Positive Review:")
            max_text = texts[sorted_scores[0][0]]
            if len(max_text) > 300:
                max_text = max_text[:300] + "..."
            print(max_text)
        else:
            print("No Positive Reviews")
        print()

        if sorted_scores[-1][1] < -0.25:
            print("Sample Negative Review:")
            min_text = texts[sorted_scores[-1][0]]
            if len(min_text) > 300:
                min_text = min_text[:300] + "..."
            print(min_text)
        else:
            print("No Negative Reviews")
        print("\n\n")

worst = sorted(courses.items(), key=lambda x: x[1], reverse=False)[:5]
print(worst)
print()

best = sorted(courses.items(), key=lambda x: x[1], reverse=True)[:5]
print(best)
print()

"""
fig, ax = plt.subplots()
ax.hist(courses.values(), bins=20)
plt.show()
"""
