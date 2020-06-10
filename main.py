from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

import sqlalchemy
from sqlalchemy.orm import sessionmaker

from database import models

# initialize
engine = sqlalchemy.create_engine("sqlite:///:memory:", echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

from pydantic import BaseModel


api = FastAPI()


class Season(BaseModel):
    season_code: str
    term: str
    year: int

    class Config:
        example = {
            "example": {"season_code": "202001", "term": "spring", "year": "2020",}
        }


@api.get("/api/seasons/")
async def list_seasons():
    return {"message": "hello world"}


@api.get("/api/courses/search")
async def search_courses():
    return {"message": "hello world"}


@api.get("/api/courses/details/{course_id}")
async def course_details(course_id: int):
    return {"message": "hello world"}


@api.get("/api/evaluations/course/{course_id}")
async def course_evaluations(course_id: int):
    return {"message": "hello world"}
