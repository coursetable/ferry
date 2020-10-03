# Ferry

A crawler for Yale courses and evaluation data. Integrates with Coursetable.

## Setup

Dependencies:

- Python 3.8 or newer.
- graphviz, which we use to generate schema diagrams.
- Postgres, our backend database that enables fast queries.

These steps will install the necessary system dependencies, setup the virtualenv, install Python package dependencies, and bootstrap the project.

```
# macOS
export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/opt/openssl/lib/
brew install graphviz postgresql
poetry install

# Ubuntu
sudo apt-get install python-dev pkg-config graphviz libgraphviz-dev libpq-dev
poetry install
```

Known issues:

- On post-Sierra versions of macOS, running `poetry install` may report an error during `psycopg2` installation stating that `ld: library not found for -lssl`. To fix this, make sure OpenSSL installed (for instance, after running `brew install openssl`), and add its libraries to the path with `export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/opt/openssl/lib/`.

## Data Files

The data files are stored in the `data` directory. This includes course and evaluations data back to 2009, caches, raw HTML for debugging, and course demand statistics.

The course evaluations data are private, and should only be accessible to Yale students and faculty. As such, we store these files in a private Git submodule.

If you want to use this data and already have access to it via official Yale systems, but don't want to crawl it yourself, please reach out and we can grant access.

## Ferry Design

We want the crawler to be reproducible and reliable. As such, we designed the crawling pipeline as a number of stages, and each stage can be individually inspected or rerun without impacting the other stages.

- Stage 1: Pulling course data into files.
- Stage 2: Pulling evaluations data into files.
- Stage 3: Importing the data files into a Postgres database.
- Stage 4: Verifying the imported data and running post-processing steps.

Stages 1 and 2 are documented in the [retrieval docs](docs/1_retrieval.md), and implemented in the `ferry/crawler` directory. We also needed to migrate data from the previous Coursetable databases in a similar fashion. This process is documented in the [migration docs](docs/0_migration.md), and implemented in the `ferry/migration` directory.

Stages 3 and 4 make use of the database, which is documented in [parsing docs](docs/2_parsing.md). Moreover, the database schema is defined with SQLAlchemy in `ferry/database/models.py`. Stage 3 is implemented by `ferry/importer.py`, and Stage 4 is implemented by `ferry/computed.py`. Both stages 3 and 4 are fully idempotent.

## TODO

- import course demand statistics
- transition everything from /private to config_private.py
