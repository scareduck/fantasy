#!/bin/bash
# Push the local fantasy DB to the read-only web mirror on the droplet.
# Runs after fantasy-run-all / fantasy-espn so the site stays current.
set -euo pipefail

REMOTE=rlm@escr2.scareduck.com

mariadb-dump --single-transaction --no-tablespaces fantasy \
  | ssh "$REMOTE" "mariadb fantasy"

# Pull latest web files in case UI changed.
ssh "$REMOTE" "cd /var/www/fantasy && git pull --ff-only --quiet"
