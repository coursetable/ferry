#!/bin/bash
set -euo pipefail

PING_URL="https://hc-ping.com/fd248442-b2ea-457c-9c52-a087eaaf80bd"

curl -fsS -m 10 --retry 5 -o /dev/null "${PING_URL}/start"

export TERM=dumb
source .venv/bin/activate
git pull
python main.py -f config/release_courses.yml

curl -fsS -m 10 --retry 5 -o /dev/null "${PING_URL}$([ $? -ne 0 ] && echo -n /fail)"