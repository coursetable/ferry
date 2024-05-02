#!/bin/bash
set -euo pipefail

doppler setup -p coursetable -c prd

doppler run --command "docker-compose -f docker-compose.yml up --build -d"
