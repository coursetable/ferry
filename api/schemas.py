from typing import List

from pydantic import BaseModel

class Season(BaseModel):
    season_code: str
    term: str
    year: int

    class Config:
        orm_mode = True
        example = {
            "example": {"season_code": "202001", "term": "spring", "year": "2020",}
        }