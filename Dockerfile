# see https://stackoverflow.com/questions/53835198/integrating-python-poetry-with-docker
FROM python:3.8-slim-buster as base

WORKDIR /app

FROM base as builder

# install dependencies
RUN apt-get update
RUN apt-get -y install gcc g++ musl-dev libffi-dev libpq-dev libhdf5-dev python-tables
RUN apt-get -y install graphviz libgraphviz-dev

# Install poetry:
RUN python -m pip install poetry

# activate venv
RUN python -m venv /venv

# Copy in the config files:
COPY pyproject.toml poetry.lock ./
RUN poetry export --without-hashes -f requirements.txt | /venv/bin/pip install -r /dev/stdin

COPY . .
RUN poetry build && /venv/bin/pip install dist/*.whl

FROM base as final

COPY --from=builder /venv /venv
