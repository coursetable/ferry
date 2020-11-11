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

# load .env file if present
if [ -f .env ]; then
    announce "Loading environment config"
    source .env
    echo 'done loading'
    echo
fi

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

announce "Parsing all evaluations"
poetry run python ./ferry/crawler/parse_ratings.py

announce "Fetching and parsing demand statistics for latest year"
poetry run python ./ferry/crawler/fetch_demand.py -s LATEST_3

announce "Pushing data changes to remote"
pushd data
git add -A
git commit -m "automatic update on $(date)"
git push
popd

announce "Constructing tables"
poetry run python ./ferry/transform.py

announce "Staging tables"
poetry run python ./ferry/stage.py

announce "Deploying staged tables"
poetry run python ./ferry/deploy.py

announce "Regenerating static files on server"
curl -H "X-FERRY-SECRET: ${FERRY_SECRET}" https://coursetable.com/api/catalog/refresh
