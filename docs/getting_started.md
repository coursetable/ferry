# Getting started with Ferry

## Initial development environment setup

Before running, make sure the following are installed and configured:

- Python 3.8 or newer.
- [Poetry](https://python-poetry.org/docs/), which we use for Python dependency management.
- [Graphviz](https://graphviz.org/download/), which we use to generate schema diagrams. (Note: this is a development dependency required only for generating [the database diagram](docs/db_diagram.png) and can be ignored.)
- [Docker](https://docs.docker.com/get-docker/), which we use to run our Postgres database.

If your default Python version is below 3.8, we recommend that you use pyenv to create a virtual environment rather than adding yet another Python installation to your path. For instance, creating and activating an environment with Python 3.8.6 can be done with

```bash
pyenv install 3.8.6
pyenv local 3.8.6  # Activate Python 3.8.6 for the current project
```

To install Poetry, make sure Python is installed and run

```bash
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
```

Graphviz and Postgres can be installed on macOS and Ubuntu as follows:

```bash
# macOS
export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/opt/openssl/lib/
brew install graphviz

# Ubuntu
sudo apt-get install build-essential python3-dev pkg-config graphviz libgraphviz-dev libpq-dev
```

Finally, to install Python dependencies via Poetry, run

```bash
poetry install
```

from anywhere within this project.

## Starting up

To run the Python scripts correctly, activate the virtual environment created by Poetry by running

```bash
poetry shell
```

The stages prior to the database import consist of Python scripts, so Poetry alone is sufficient. However, to run the database importer and additional post-processing steps, the Docker container, which provides the Postgres database, must be started. This can be done by running

```bash
docker-compose up
```

from the project root. This will automatically download and install the Docker files and start the Postgres server.

Note that CourseTable proper interacts with ferry via an additional GraphQL endpoint provided by Hasura on CourseTable's end (see [coursetable/docker/docker-compose.yml](https://github.com/coursetable/coursetable/blob/master/docker/docker-compose.yml)). For development purposes, you can also host the GraphQL endpoint from ferry by running

```bash
docker-compose -f docker-compose.yml -f docker-compose.hasura.yml up
```

This command will start Hasura in addition to the Postgres container specified in the default compose file.

## Starting from scratch

To illustrate how the database might be constructed, we provide an workflow to run to build everything from scratch (assuming all dependencies have been accounted for).

### Retrieval

To extract data from Yale's websites, we use the scripts provided in `/ferry/crawler`. 

1. Before retrieving any data, we have to have a sense of which semesters, or **seasons**, we want to fetch. To retrieve a list of seasons, we run `fetch_seasons.py`. This gives us a list of valid seasons for course listings and demand statistics (we get the list of seasons for evaluations separately).
2. To retrieve our classes, we run `fetch_classes.py`, which downloads raw JSON data from Yale.
3. To retrieve evaluations, we run `fetch_ratings.py`. For each valid class found in `fetch_classes.py`, this script will download all evaluation info, namely categorical and written evaluation responses. Yale credentials are required for this step – see `/ferry/config.py` for details on setting these.
4. To retrieve demand statistics, we also need a list of course subject codes that the demand statistics are indexed by. These can be found using `fetch_subjects.py`. Once this has been done, we can get demand subjects using `fetch_demand.py`.

Note that `fetch_classes.py`, `parse_classes.py`, `fetch_ratings.py`, `fetch_subjects.py`, and `fetch_demand.py` all have a `--season` argument that allows one to manually filter which seasons to retrieve. This script is useful for periodic updates in which we don't need to process older seasons (see [refresh.sh](/refresh_courses.sh)) and for testing.

### Preprocessing

We also preprocess our classes and ratings data to make them easier to import. In particular:

1. We run `parse_classes.py`, which does some pre-processing such as parsing syllabus links and cross-listings from various HTML fields. 
2. We run `parse_ratings.py`, which takes all of the individual ratings JSONs per class and aggregates them into CSV tables for all questions, narrative (written) evaluations, categorical evaluations, and enrollment/response statistics. This step also calculates sentiment scores on the narrative evaluations using [VADER](https://github.com/cjhutto/vaderSentiment).

### Importation

As mentioned above, the only step here is to run `/ferry/stage.py`. With our full dataset, this takes about 2 minutes.

### Post-processing

After the initial tables have been staged in Postgres, we run `/ferry/deploy.py` to do the following:

- Check invariants (e.g. the season codes in our listings and courses tables match)
- If checks are successful, promote our staging tables to the main ones
- Reindex the entire database
- Regenerate materialized tables and add full-text search capability

With our full dataset, this takes about a minute.

## Troubleshooting

- On post-Sierra versions of macOS, running `poetry install` may report an error during `psycopg2` installation stating that `ld: library not found for -lssl`. To fix this, make sure OpenSSL is installed (such as through `brew install openssl`) and rerun the above command block.
- On macOS Big Sur, the new version number may cause Poetry to attempt to compile several modules such as NumPy and SpaCy from scratch rather than using prebuilt binaries. This can be avoided by setting the flag `SYSTEM_VERSION_COMPAT=1`.
- ARM Macs currently do not have good support for NumPy and several other compiled Python packages, so we recommend that you [run terminal with Rosetta2](https://www.notion.so/Run-x86-Apps-including-homebrew-in-the-Terminal-on-Apple-Silicon-8350b43d97de4ce690f283277e958602) or use the provided [VSCode DevContainer](https://code.visualstudio.com/docs/remote/containers).

