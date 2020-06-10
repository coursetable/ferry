from fastapi import Depends, FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

from database import models
from api import crud, schemas

from typing import List

# initialize
engine = sqlalchemy.create_engine("sqlite:///db/tmp.db", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

api = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@api.get("/api/seasons/", response_model=List[schemas.Season])
async def list_seasons(db: Session = Depends(get_db)):
    return list(crud.get_seasons(db))


@api.get("/api/courses/search")
async def search_courses():
    return {"message": "hello world"}


@api.get("/api/courses/details/{course_id}")
async def course_details(course_id: int):
    return {"message": "hello world"}


@api.get("/api/evaluations/course/{course_id}")
async def course_evaluations(course_id: int):
    return {"message": "hello world"}
