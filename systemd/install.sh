#!/usr/bin/env bash
# Install and enable Fantasy Baseball systemd user timers.
# Run once after cloning or after updating service files.
set -euo pipefail

UNIT_DIR="${HOME}/.config/systemd/user"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "${UNIT_DIR}"

for unit in fantasy-sync fantasy-espn fantasy-report; do
    cp "${SCRIPT_DIR}/${unit}.service" "${UNIT_DIR}/"
    cp "${SCRIPT_DIR}/${unit}.timer"   "${UNIT_DIR}/"
    echo "Installed ${unit}.service and ${unit}.timer"
done

systemctl --user daemon-reload

for unit in fantasy-sync fantasy-espn fantasy-report; do
    systemctl --user enable --now "${unit}.timer"
    echo "Enabled and started ${unit}.timer"
done

echo ""
echo "All timers installed. Current status:"
systemctl --user list-timers fantasy-*
