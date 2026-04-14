#!/usr/bin/env python3
"""Run the full Fantasy Baseball sync pipeline in sequence."""
from __future__ import annotations

import sys

from scripts.espn_forecaster_sync import main as espn_main
from scripts.pitcher_report import main as report_main
from scripts.yahoo_sync import main as sync_main


def main() -> int:
    print("=== Step 1: Yahoo sync ===")
    rc = sync_main()
    if rc:
        print(f"Yahoo sync failed (exit {rc}), aborting.", file=sys.stderr)
        return rc

    print("\n=== Step 2: ESPN forecaster sync ===")
    rc = espn_main()
    if rc:
        print(f"ESPN sync failed (exit {rc}), aborting.", file=sys.stderr)
        return rc

    print("\n=== Step 3: Pitcher reports ===")
    rc = report_main()
    if rc:
        print(f"Report failed (exit {rc}).", file=sys.stderr)
        return rc

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
