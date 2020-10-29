#!/bin/bash
set -euo pipefail

# Bold headers for readability. Fails gracefully when the terminal doesn't support it.
bold=$(tput bold || true)
normal=$(tput sgr0 || true)

announce () {
  printf "\n${bold}[$1]${normal}\n"
}

# go to ferry root for poetry to work
cd $(dirname $0)

# ensure the data is up to date
(cd data && git checkout master && git pull)

# install any new dependencies
poetry install

announce "Fetching course+demand seasons"
poetry run python ./ferry/crawler/fetch_seasons.py

announce "Fetching classes for latest year"
poetry run python ./ferry/crawler/fetch_classes.py -s LATEST_3

announce "Parsing all classes"
poetry run python ./ferry/crawler/parse_classes.py

announce "Fetching and parsing demand statistics for latest year"
poetry run python ./ferry/crawler/fetch_demand.py -s LATEST_3

announce "Pushing data changes to remote"
pushd data
git add -A
git commit -m "automatic update on $(date)"
git push
popd

announce "Importing courses to database"
poetry run python ./ferry/importer.py --mode courses

announce "Importing demand to database"
poetry run python ./ferry/importer.py --mode demand

announce "Generating computed database fields"
poetry run python ./ferry/computed.py
