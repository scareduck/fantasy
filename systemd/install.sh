#!/usr/bin/env bash
# Install and enable Fantasy Baseball systemd user timers.
# Run once after cloning or after updating service files.
set -euo pipefail

UNIT_DIR="${HOME}/.config/systemd/user"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "${UNIT_DIR}"

# Disable and remove the old individual units if present
for unit in fantasy-sync fantasy-espn fantasy-report; do
    if systemctl --user is-enabled "${unit}.timer" &>/dev/null; then
        systemctl --user disable --now "${unit}.timer"
        echo "Disabled old ${unit}.timer"
    fi
    rm -f "${UNIT_DIR}/${unit}.service" "${UNIT_DIR}/${unit}.timer"
done

cp "${SCRIPT_DIR}/fantasy-run-all.service"       "${UNIT_DIR}/"
cp "${SCRIPT_DIR}/fantasy-run-all.timer"         "${UNIT_DIR}/"
echo "Installed fantasy-run-all.service and fantasy-run-all.timer"

cp "${SCRIPT_DIR}/fantasy-game-log-sync.service" "${UNIT_DIR}/"
cp "${SCRIPT_DIR}/fantasy-game-log-sync.timer"   "${UNIT_DIR}/"
echo "Installed fantasy-game-log-sync.service and fantasy-game-log-sync.timer"

cp "${SCRIPT_DIR}/fantasy-svh-report.service"    "${UNIT_DIR}/"
cp "${SCRIPT_DIR}/fantasy-svh-report.timer"      "${UNIT_DIR}/"
echo "Installed fantasy-svh-report.service and fantasy-svh-report.timer"

systemctl --user daemon-reload

systemctl --user enable --now fantasy-run-all.timer
echo "Enabled and started fantasy-run-all.timer"

systemctl --user enable --now fantasy-game-log-sync.timer
echo "Enabled and started fantasy-game-log-sync.timer"

systemctl --user enable --now fantasy-svh-report.timer
echo "Enabled and started fantasy-svh-report.timer"

echo ""
echo "All timers installed. Current status:"
systemctl --user list-timers fantasy-*
