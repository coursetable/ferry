# pylint: skip-file
import numpy as np
import pandas as pd

from ferry import config, database

courses = pd.read_sql_table("courses", con=database.Engine)  # type: ignore
listings = pd.read_sql_table("listings", con=database.Engine)  # type: ignore

# set course id as index for both
courses = courses.set_index("course_id")
listings = listings.set_index("course_id")

# pull out variables of interest
# use explode() to expand lists
areas = "AREA_" + courses["areas"].explode().dropna().astype(str)
skills = "SKILL_" + courses["skills"].explode().dropna().astype(str)
schools = "SCHOOL_" + courses["school"].dropna().astype(str)
credits = "CREDIT_" + courses["credits"].dropna().astype(str)
subjects = "SUBJECT_" + listings["subject"].dropna().astype(str)


# helper function to binarize a series into matrix form
def pivot_binary(series):
    df = pd.DataFrame(series)

    # make the old index a column for pivoting
    index_name = df.index.name
    df = df.reset_index(drop=False)

    # dummy value for pivoting
    df["value"] = 1
    pivot = pd.pivot_table(
        df, values="value", index=index_name, columns=series.name, fill_value=0
    )

    # use half-precision floats to allow NaNs
    pivot = pivot.astype(np.float16)

    return pivot


# construct binary matrices per course
# areas = pd.pivot_table(areas, values="value", columns="", fill_value=0)
areas = pivot_binary(areas)
skills = pivot_binary(skills)
schools = pivot_binary(schools)
credits = pivot_binary(credits)
subjects = pivot_binary(subjects)

# join all binary matrices along variable axis
context_vectors = pd.concat(  # type: ignore
    [areas, skills, schools, credits, subjects], join="outer", axis=1
)

# fill missing values
context_vectors = context_vectors.fillna(0)

# export to HDF5
context_vectors.to_hdf(  # type: ignore
    config.DATA_DIR / "description_embeddings/context_vectors.h5",
    key="context_vectors",
    mode="w",
)
