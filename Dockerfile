# see https://stackoverflow.com/questions/53835198/integrating-python-poetry-with-docker
FROM python:3.8-slim-buster as base

WORKDIR /app
 
FROM base as final

# install dependencies
RUN apt-get update
# low-level stuff
RUN apt-get -y install gcc g++ musl-dev libffi-dev libpq-dev
# for pytables
RUN apt-get -y install libhdf5-dev python-tables
# for network visualization
RUN apt-get -y install graphviz libgraphviz-dev
# for Java (required by tabula)
RUN apt-get -y install openjdk-8-jre

# Install poetry:
RUN python -m pip install poetry

# Copy everything
COPY . .
