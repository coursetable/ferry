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
Some of the intermediate JSON files generated by the crawler and migrator in `/api_output` take several hours to fetch from the Yale API and/or CourseTable.
We have uploaded these compressed folders to a [Google Drive folder](https://drive.google.com/drive/u/1/folders/14wl5ibpeLTQaVHK-DNTfLUaWb1N7lY7M).

[optional] We have a couple scripts to automate this process - they currently require `rclone` to be [installed](https://rclone.org/install/). Once this is set up, run `rclone config` to set up the remote, which must be called `coursetable`. The client ID and client secret can be accessed via this [OAuth client](https://console.developers.google.com/apis/credentials/oauthclient/834119546246-kga3min5p74ks3rdmceu68librsfj5oc.apps.googleusercontent.com?project=ferry-280404&supportedpurview=project) (ask for access). Use Coursetable's Google Account to authenticate with rclone.
**To pull the files**, run the `drive_fetch.sh` script.
**To update the files in drive**, run the `drive_push.sh` script.

## Ferry Design

We want the crawler to be reproducible and reliable. As such, we designed the crawling pipeline as a number of stages, and each stage can be individually inspected or rerun without impacting the other stages.

- Stage 1: Pulling course data into files.
- Stage 2: Pulling evaluations data into files.
- Stage 3: Importing the data files into a Postgres database.
- Stage 4: Verifying the imported data and running post-processing steps.

Stages 1 and 2 are documented in the [retrieval docs](docs/1_retrieval.md), and implemented in the `ferry/crawler` directory. We also needed to migrate data from the previous Coursetable databases in a similar fashion. This process is documented in the [migration docs](docs/0_migration.md), and implemented in the `ferry/migration` directory.

Stages 3 and 4 make use of the database, which is documented in [parsing docs](docs/2_parsing.md). Moreover, the database schema is defined with SQLAlchemy in `ferry/database/models.py`. Stage 3 is implemented by `ferry/importer.py`, and Stage 4 is implemented by `ferry/computed.py`. Both stages 3 and 4 are fully idempotent.

## TODO
- save the raw HTML from OCE in the evaluations crawler
- import course demand statistics
- transition everything from /private to config_private.py
- get full names of subjects/departments in addition to the abbreviations (could be done in computed.py with a static table)
