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

cp "${SCRIPT_DIR}/fantasy-run-all.service" "${UNIT_DIR}/"
cp "${SCRIPT_DIR}/fantasy-run-all.timer"   "${UNIT_DIR}/"
echo "Installed fantasy-run-all.service and fantasy-run-all.timer"

systemctl --user daemon-reload

systemctl --user enable --now fantasy-run-all.timer
echo "Enabled and started fantasy-run-all.timer"

echo ""
echo "All timers installed. Current status:"
systemctl --user list-timers fantasy-*
