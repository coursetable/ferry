# pylint: skip-file
import pandas as pd
import plotly.express as px

from ferry import config

courses = pd.read_csv(
    config.DATA_DIR
    / "description_embeddings/courses_description_deduplicated_umap.csv",
    index_col=0,
)

courses["full_title"] = (
    courses["title"] + " (" + courses["season_code"].astype(str) + ")"
)

fig = px.scatter(
    courses.dropna(subset=["school"]),
    x="umap1",
    y="umap2",
    render_mode="webgl",
    hover_name="full_title",
    color="school",
    width=960,
    height=960,
)

fig.update_traces(
    marker=dict(size=6, opacity=0.8), marker_line=dict(width=0, color="DarkSlateGray")
)

fig.write_html(str(config.DATA_DIR / "description_embeddings/all_courses_umap.html"))

fig = px.scatter(
    courses[courses["season_code"] == 202003],
    x="umap1",
    y="umap2",
    render_mode="webgl",
    hover_name="title",
    color="school",
    width=960,
    height=960,
)

fig.update_traces(
    marker=dict(size=6, opacity=0.8), marker_line=dict(width=0, color="DarkSlateGray")
)

fig.write_html(str(config.DATA_DIR / "description_embeddings/202003_umap.html"))
