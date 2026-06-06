#!/bin/bash
# Data sync: steps 1-3 of run_all.py (no reports).
# Invoked by sync.php via sudo -u rlm.
set -euo pipefail

export HOME=/home/rlm
export PATH=/home/rlm/.local/bin:/usr/local/bin:/usr/bin:/bin

echo "=== Step 1: Yahoo sync ==="
fantasy-sync --all-rosters

echo ""
echo "=== Step 2: Batter season stats ==="
fantasy-batter-sync

echo ""
echo "=== Step 3: Pitcher season stats ==="
fantasy-pitcher-stats

echo ""
echo "=== Step 4: ESPN forecaster sync ==="
fantasy-espn

echo ""
echo "=== Step 5: MLB schedule sync ==="
fantasy-mlb-schedule

echo ""
echo "=== Done ==="
