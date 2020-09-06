#!/bin/bash

# Exit upon any error.
set -e

# bold headers for readability
bold=$(tput bold)
normal=$(tput sgr0)

announce () {
  printf "\n${bold}[$1]${normal}\n"
}

# go to ferry root for poetry to work
cd $(dirname $0)

# install any new dependencies
poetry install

announce "Fetching course+demand seasons"
poetry run python ./ferry/crawler/fetch_seasons.py

announce "Fetching classes for latest year"
poetry run python ./ferry/crawler/fetch_classes.py -s LATEST_3

announce "Parsing all classes"
poetry run python ./ferry/crawler/parse_classes.py

# announce "Fetching ratings for latest year"
# poetry run python ./ferry/crawler/fetch_ratings.py -s LATEST_3

# announce "Fetching demand statistics for latest year"
# poetry run python ./ferry/crawler/fetch_demand.py -s LATEST_3

announce "Creating and uploading Google Drive backup"
sh ./drive_push.sh

announce "Importing to database"
poetry run python ./ferry/importer.py

announce "Generating computed database fields"
poetry run python ./ferry/computed.py
