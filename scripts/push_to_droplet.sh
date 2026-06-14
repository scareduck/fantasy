#!/bin/bash
# Push the local fantasy DB to the read-only web mirror on the droplet.
# Runs after fantasy-run-all / fantasy-espn so the site stays current.
set -euo pipefail

REMOTE=rlm@escr2.scareduck.com

mysqldump --single-transaction --no-tablespaces fantasy \
  | ssh "$REMOTE" "mysql fantasy"

# Pull latest web files in case UI changed.
ssh "$REMOTE" "cd /home/rlm/src/fantasy && git pull --ff-only --quiet"
