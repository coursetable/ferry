# Getting started with Ferry

## Initial development environment setup

1. Install [Visual Studio Code](https://code.visualstudio.com/Download).

   > **For Windows**: When installing, make sure `Add to PATH (requires shell restart)` option is checked. You can make sure that it was added by going to Control Panel -> System and Security -> System -> Advanced System Settings -> Environment Variables... -> Under your user variables double-click `Path`. Here you should see an entry that looks like `C:\Users\<your-user-name-here>\AppData\Local\Programs\Microsoft VS Code\bin`. If you don't, click `New` and add it here.

2. Join our GitHub organization and clone the repository.

   - Make sure that you're added to the [CourseTable GitHub organization](https://github.com/coursetable).

   - Clone the [coursetable/ferry repository](https://github.com/coursetable/ferry) by running `git clone https://github.com/coursetable/ferry.git`.

     > **For Windows**: Make sure to clone the repository in your Linux filesystem in Ubuntu using Windows Terminal (NOT your Windows filesystem). This will allow React hot reloading to work. After cloning, cd to the repository. Open the repository in VSCode by running the command `code .`. This should open it using WSL, and you should see a green bar on the bottom left of your VSCode editor that says `WSL: Ubuntu-20.04`. Also, make sure that the bar in the bottom right says `LF` and not `CRLF`.

   - **Note**: If you'd also like to clone the private submodule containing all our scraped data, run `git submodule init` followed by `git submodule update` within the repository. Access to this submodule requires you to be a member of our GitHub organization.

3. Install Docker.

   - MacOS or Windows: Install [Docker Desktop](https://www.docker.com/products/docker-desktop).

     > **For Windows**: Make sure `Enable WSL 2 Windows Features` is checked during installation.

   - Linux: Install [Docker CE](https://docs.docker.com/engine/install/) and [Docker Compose](https://docs.docker.com/compose/install/).

4. Install Postgres. Although we run Postgres through Docker, some bindings are required by the SQLAlchemy ORM that interfaces between Python and the database.

   - MacOS: run `brew install postgres`.
   - Linux: run `sudo apt-get install postgresql libpq-dev`.
   - Windows: use the [interactive installer](https://www.postgresql.org/download/windows/).

5. Install Java (8 or newer). This is only used for parsing discussion sections – we use a tool called [tabula](https://tabula.technology/) to automatically extract these from a PDF that Yale posts.

   - MacOS: run `brew install openjdk@8`.
   - Linux: run `sudo apt-get install openjdk-8-jre`.
   - Windows: use the [interactive installer](http://openjdk.java.net/install/).

6. Install graphviz.

   - MacOS: run `brew install graphviz`.
   - Linux: run `sudo apt-get install graphviz libgraphviz-dev `.
   - Windows: use the [interactive installer](https://graphviz.org/download/).

   **Note**: you can skip this if you do not want to generate the database diagram, for which you will need to also run `poetry install --no-dev` later.

7. Install Python 3.8 or newer. If not already installed, download an installer from the [Python site](https://www.python.org/downloads/). If you already have a Python installation below 3.8 but don't want to add another one, use Pyenv to create a virtual environment with a version of your choice:

   ```shell
   pyenv install 3.8.6
   pyenv local 3.8.6  # Activate Python 3.8.6 for the current project
   ```

   Alternatively, you can configure Poetry to use a preset Python version with `poetry env use <python_command>`.

   **Note**: Python3.9 on MacOS is currently incompatible, as NumPy fails to install due to an OS version error. This should be fixed once NumPy 1.20 is released in a few weeks.

8. Install [Poetry](https://python-poetry.org/), the package manager we use for Python dependencies. To install, make sure Python is installed and added to PATH, and run

   ```shell
   curl -sSL https://install.python-poetry.org | python3 -
   ```

9. Install Python dependencies with Poetry: run `poetry install` from the repository root.

## Aside: a quick explainer on docker-compose

`docker-compose` is a tool we use to orchestrate a bunch of different things, all running in parallel. It also enables us to avoid most cross-platform compatibility issues.

Our setup is declared in the [docker-compose.yml](https://github.com/coursetable/coursetable/blob/master/docker/docker-compose.yml) file.

Some useful commands:

- `docker-compose up` starts all the services
- `docker-compose up -d` starts everything in the background
- `docker-compose ps` tells you what services are running
- `docker-compose stop` stops everything
- `docker-compose down` stops and removes everything
- `docker-compose restart` restarts everything
- `docker-compose logs -f` gets and "follows" (via `-f`) the logs from all the services. It's totally safe to control-C on this command - it won't stop anything
- `docker-compose logs -f <service>` gets the logs for a specific service. For example, `docker-compose logs -f api` gets the logs for the backend API.
- `docker-compose build` builds all the services. This probably won't be necessary for our development environment, since we're building everything on the fly

## Starting Ferry

1. Activate Poetry by running `poetry shell`. Alternatively, you can run a single script from within the Poetry environment through `poetry run <command>` while within Ferry.

2. Start Docker by running `docker-compose up`.

CourseTable proper interacts with Ferry via an additional GraphQL endpoint provided by [Hasura](https://hasura.io/) on CourseTable's end (see [coursetable/docker/docker-compose.yml](https://github.com/coursetable/coursetable/blob/master/docker/docker-compose.yml)). For development purposes, you can optionally host the GraphQL endpoint from Ferry by running

```bash
docker-compose -f docker-compose.yml -f docker-compose.hasura.yml up
```

This command will start Hasura in addition to the Postgres container specified in the default compose file. The Hasura console can be viewed at [localhost:8080](https://localhost:8080).

## Additional steps

1. The FastText embeddings are quite large (~800MB), so we have excluded them from the data directory. You can generate these by running `poetry run python /ferry/embed/embed_fasttext.py --retrain`, assuming you have already prepared the corpus with `/ferry/embed/assemble_corpus.py`. This step should take about 10 minutes on recent hardware.

## Troubleshooting

- On MacOS, setup may report an error that OpenSSL headers are missing. To fix this, try installing OpenSSL from Homebrew and run

  ```shell
  export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/opt/openssl/lib/
  ```

- On post-Sierra versions of macOS, running `poetry install` may report an error during `psycopg2` installation stating that `ld: library not found for -lssl`. To fix this, make sure OpenSSL is installed and linked as described above and rerun the above command block.

- On macOS Big Sur, the new version number may cause Poetry to attempt to compile several modules such as NumPy and SpaCy from scratch rather than using prebuilt binaries. This can be avoided by setting the flag `SYSTEM_VERSION_COMPAT=1`.

- ARM Macs currently do not have good support for NumPy and several other compiled Python packages, so we recommend that you [run terminal with Rosetta2](https://www.notion.so/Run-x86-Apps-including-homebrew-in-the-Terminal-on-Apple-Silicon-8350b43d97de4ce690f283277e958602) or use the provided [VSCode DevContainer](https://code.visualstudio.com/docs/remote/containers) (see below).

## Running the DevContainer
The entire development environment can be run inside of [a Docker image](https://hub.docker.com/repository/docker/coursetable/ferry) that we have set up with all of the dependencies preinstalled. Using VSCode's DevContainer feature, Ferry can be developed from this container. Note that after starting the DevContainer, you still need to run `poetry install` and `poetry shell` – the container only contain all non-Python background dependencies.

When starting VSCode, the editor should automatically detect the DevContainer and prompt with the option to 'Reopen folder to develop in a container'. Otherwise, try installing the [Remote Development extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack) and search for 'Reopen-Containers: Reopen Folder in Container' in the Command Palette (`ctrl-shift-b`).

If making changes to the Dockerfile, make sure to build and push as follows:

```shell
# build without context
docker image build -t coursetable/ferry - < Dockerfile

# push to Docker hub (authentication required)
# to authenticate, use `docker login` after having joined the Docker org
docker push coursetable/ferry:latest
```

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

With all our data preprocessed, we can now begin assembly into tables for import into the database. This is performed by `/ferry/transform.py`, which pulls together all of these previous files and outputs a collection of CSVs in `/data/importer_dumps`.

After these tables have been constructed, we run `/ferry/stage.py` to read and copy our CSVs into Postgres. With our full dataset, this takes about two minutes.

### Post-processing

After the initial tables have been staged in Postgres, we run `/ferry/deploy.py` to do the following:

- Check invariants (e.g. the season codes in our listings and courses tables match).
- If checks are successful, promote our staging tables to the main ones.
- Reindex the entire database.
- Regenerate materialized tables (namely, `computed_listing_info`).
- Ping CourseTable to download JSON dumps for courses per season. These JSONs are used for fast catalog search, which is done on the frontend rather than via requests with Ferry.

With our full dataset, this takes about a minute.
