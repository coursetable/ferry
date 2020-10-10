## Setup

```
poetry install
```

## Usage

### Download Data

With CourseTable running locally, 

```
cd fetch
poetry run python fetch_evals.py
poetry run python fetch_questions.py
cd ..
```

### Running Models

```
poetry run python keywords.py
poetry run python sentiment.py
poetry run python similarity.py
poetry run python summarization.py
```
