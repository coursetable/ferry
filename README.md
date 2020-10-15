# Ferry

A crawler for Yale courses and evaluation data. Integrates with Coursetable.

## Design

We want the crawler to be reproducible and reliable. As such, we designed the crawling pipeline as a number of stages able to be independently inspected and rerun.

- **Extraction**: We pull and preprocess raw data from Yale's websites to fetch the following:
  - Course listings
  - Course evaluations
  - Course demand statistics
- **Importation**: We import the preprocessed data files into our Postgres database.
- **Post-processing**: We verify the imported data and compute/modify additional attributes such as numerical ratings.

Extraction is documented in the [retrieval docs](docs/1_retrieval.md) and implemented in the `ferry/crawler` directory. We also needed to migrate data from the previous CourseTable databases in a similar fashion. This process is documented in the [migration docs](docs/0_migration.md) and implemented in the `ferry/migration` directory.

Importation and post-processing make use of the database, which is documented in [parsing docs](docs/2_parsing.md). Moreover, the database schema is defined with SQLAlchemy in `ferry/database/models.py`. Importation is implemented by `ferry/importer.py`, and post-processing is implemented by `ferry/computed.py`. Both importation and post-processing are fully idempotent.

## Dependencies

Before running, make sure the following are installed and configured:

- Python 3.8 or newer.
- [Poetry](https://python-poetry.org/docs/), which we use for Python dependency management.
- [Graphviz](https://graphviz.org/download/), which we use to generate schema diagrams.
- [Postgres](https://www.postgresql.org/download/), our backend database that enables fast queries.
- [Docker](https://docs.docker.com/get-docker/), which we use to host the backend database.

To install Poetry, make sure Python is installed and run

```
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
```

Graphviz and Postgres can be installed on macOS and Ubuntu as follows:

```
# macOS
export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/opt/openssl/lib/
brew install graphviz postgresql

# Ubuntu
sudo apt-get install python-dev pkg-config graphviz libgraphviz-dev libpq-dev
```

Known issues:

- On post-Sierra versions of macOS, running `poetry install` may report an error during `psycopg2` installation stating that `ld: library not found for -lssl`. To fix this, make sure OpenSSL is installed (such as through `brew install openssl`) and rerun the above command block.

To install Python dependencies via Poetry, run

```
poetry install
```

from anywhere within this project.

## Usage

To run the Python scripts correctly, activate the virtual environment created by Poetry by running

```
poetry shell
```

The stages prior to the database import consist of Python scripts, so Poetry alone is sufficient. However, to run the database importer and additional post-processing steps, the Docker container, which provides the Postgres database, must be started. This can be done by running

```
docker-compose up
```

from the project root. This will automatically download and install the Docker files and start the Postgres server.

## Data Files

The data files – outputs from the extraction stage – are stored in the `/data` directory.
The course evaluations data are private and should only be accessible to Yale students and faculty. As such, we store these files in a private Git submodule.

```
# Download data files from private repository into the /data directory.
git submodule update --init
```

This submodule includes course and evaluation data dating back to 2009 (many of which are no longer available through Yale), more recent course demand statistics, and caches and raw HTML files for debugging purposes.

_If you want to use these data but don't want to crawl it yourself, please reach out and we can grant access to our archives._

## TODO

- import course demand statistics
- transition everything from /private to config_private.py
