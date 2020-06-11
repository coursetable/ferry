from sqlalchemy.orm import Session

import sys
sys.path.append("..")

from database import models
from api import schemas

def get_seasons(db: Session):
	return db.query(models.Season).all()