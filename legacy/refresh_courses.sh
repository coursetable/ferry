#!/bin/bash
set -euo pipefail

# Bold headers for readability. Fails gracefully when the terminal doesn't support it.
bold=$(tput bold || true)
normal=$(tput sgr0 || true)

announce () {
  printf "\n${bold}[$1]${normal}\n"
}

# Check arguments
SKIP_FETCH=""
if [ $# -gt 0 ] && [ "$1" = "--skip-fetch" ]; then
	SKIP_FETCH="yes"
fi
[ "$SKIP_FETCH" ] && echo 'skipping fetch commands'
[ "$SKIP_FETCH" ] || echo 'running fetch commands'

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
# (cd data && git checkout master && git pull)

# install any new dependencies
poetry install

[ "$SKIP_FETCH" ] || {
announce "Fetching course+demand seasons"
poetry run python ./ferry/crawler/fetch_seasons.py
}

[ "$SKIP_FETCH" ] || {
announce "Fetching classes for latest year"
poetry run python ./ferry/crawler/fetch_classes.py
}

# [ "$SKIP_FETCH" ] || {
# announce "Fetching current discussion sections"
# poetry run python ./ferry/crawler/fetch_discussions.py
# }

announce "Parsing all classes"
poetry run python ./ferry/crawler/parse_classes.py

announce "Parsing all evaluations"
poetry run python ./ferry/crawler/parse_ratings.py

[ "$SKIP_FETCH" ] || {
announce "Fetching and parsing demand statistics for latest year"
poetry run python ./ferry/crawler/fetch_demand.py
}

# announce "Pushing data changes to remote"
# (
# # Via https://stackoverflow.com/a/8123841/5004662.
# cd data
# git add -A
# git diff-index --quiet HEAD || git commit -m "automatic update on $(date)"
# git push
# )

announce "Constructing tables"
poetry run python ./ferry/transform.py

announce "Staging tables"
poetry run python ./ferry/stage.py

# announce "Deploying staged tables"
# poetry run python ./ferry/deploy.py

# announce "Regenerating static files on server"
# curl --silent --show-error -H "X-FERRY-SECRET: ${FERRY_SECRET}" https://api.coursetable.com/api/catalog/refresh
