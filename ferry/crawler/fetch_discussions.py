import tabula
import pandas as pd

from ferry.config import DATA_DIR

discussions = tabula.read_pdf(
    "https://www.sis.yale.edu/buildings/Discussion_locations.pdf",
    pandas_options={"names":["section_crn","subject","number","section","info","time"]},
    multiple_tables=False,
    pages="all",
)[0]

discussions.to_csv(DATA_DIR / "discussion_sections" / "raw_sections.csv",index=False)