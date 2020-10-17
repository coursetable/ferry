# Ferry

A crawler for Yale courses and evaluation data. Integrates with Coursetable.

## Table of contents
<!--ts-->
   * [Design](#design)
   * [Dependencies](#dependencies)
   * [Usage](#usage)
   * [Data files](#data-files)
   * [Starting from scratch](#starting-from-scratch)
   * [Contributing](#contributing)
   * [TODO](#todo)
<!--te-->

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
sudo apt-get install build-essential python3-dev pkg-config graphviz libgraphviz-dev libpq-dev
```

Installing Graphviz and PyGraphViz may be a bit difficult on Windows – note that these are only used for generating the [database schema diagram](docs/db_diagram.png) in [`/ferry/generate_db_diagram.py`](/ferry/generate_db_diagram.py) and can be disregarded otherwise.

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

## Starting from scratch

To illustrate how the database might be constructed, we provide an workflow to run to build everything from scratch (assuming all dependencies have been accounted for).

### Extraction

To extract data from Yale's websites, we use the scripts provided in `/ferry/crawler`.

1. Before retrieving any data, we have to have a sense of which semesters, or **seasons**, we want to fetch. To retrieve a list of seasons, we run `fetch_seasons.py`. This gives us a list of valid seasons for course listings and demand statistics (we get the list of seasons for evaluations separately).
2. To retrieve our classes, we run `fetch_classes.py`, which downloads raw JSON data from Yale, followed by `parse_classes.py`, which does some pre-processing such as parsing syllabus links and cross-listings from various HTML fields. 
3. To retrieve evaluations, we run `fetch_ratings.py`. For each class found, this script will download all evaluation info, namely categorical and written evaluation responses.
4. To retrieve demand statistics, we also need a list of course subject codes that the demand statistics are indexed by. These can be found using `fetch_subjects.py`. Once this has been done, we can get demand subjects using `fetch_demand.py`.

Note that `fetch_classes.py`, `parse_classes.py`, `fetch_ratings.py`, and `fetch_subjects.py` all have an `--season` argument that allows one to manually filter which seasons to retrieve. This script is useful for periodic updates in which we don't need to process older seasons (see [refresh.sh](https://github.com/coursetable/ferry/blob/master/refresh_courses.sh)) and for testing.

### Importation

Once extraction is complete, our data can be imported into the Postgres database. As mentioned above, the only step here is to run `/ferry/importer.py`.

### Post-processing

After the initial data has been imported into Postgres, we run `/ferry/computed.py` to do the following:

- Check invariants (e.g. the season codes in our listings and courses tables match)
- Compute numerical ratings (overall rating and workload) per course
- Compute historical ratings for courses and professors over all past offerings

## Contributing

To contribute to this repository, please create a branch and open a pull request once you are ready to merge your changes into master. 

Note that we run two Python style checks via Travis CI: [black](https://github.com/psf/black) (for general code formatting) and [isort](https://github.com/PyCQA/isort) (for import ordering). You can run these two manually or by using our provided [pre-commit](https://pre-commit.com/) configuration, which can be installed after activating the Poetry enviroment with

```
pre-commit install
```

This will install pre-commit [Git hooks](https://git-scm.com/book/en/v2/Customizing-Git-Git-Hooks) that will automatically apply black and isort before you make a commit. If there are any reported changes, the initial commit will be aborted and you can re-commit to apply the changes.

## TODO

- import course demand statistics
- transition everything from /private to config_private.py
