#!/bin/bash
# Data sync: delegates to run_all.py's data-sync steps (no IL analysis, no
# reports). --no-yahoo skips the Yahoo-dependent steps (roster/player sync,
# batter/pitcher season stats, pitcher game log) while the Yahoo Fantasy API
# outage is ongoing, so ESPN forecaster sync and MLB schedule sync -- which
# don't touch Yahoo -- still run instead of the whole thing aborting at
# step 1. Drop --no-yahoo here once Yahoo sync works again.
# Invoked by sync.php via sudo -u rlm.
set -euo pipefail

export HOME=/home/rlm
export PATH=/home/rlm/.local/bin:/usr/local/bin:/usr/bin:/bin

fantasy-run-all --no-yahoo --daily

echo ""
echo "=== Done ==="
