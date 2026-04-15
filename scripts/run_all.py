#!/usr/bin/env python3
"""Run the full Fantasy Baseball sync pipeline in sequence."""
from __future__ import annotations

import sys

from fantasy.notify import send_alert
from fantasy.yahoo_auth import InteractiveAuthRequired
from scripts.espn_forecaster_sync import main as espn_main
from scripts.pitcher_report import main as report_main
from scripts.yahoo_sync import parse_args, run as sync_run


def main() -> int:
    try:
        print("=== Step 1: Yahoo sync ===")
        args = parse_args([])
        args.all_rosters = True
        rc = sync_run(args)
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

    except InteractiveAuthRequired as exc:
        msg = (
            f"Fantasy Baseball pipeline needs Yahoo re-authentication.\n\n"
            f"{exc}\n\n"
            f"Run `fantasy-sync` in a terminal to complete the login flow."
        )
        print(msg, file=sys.stderr)
        try:
            send_alert("⚠️ Fantasy Baseball: Yahoo re-authentication required", msg)
            print("Alert email sent.", file=sys.stderr)
        except Exception as mail_err:
            print(f"Could not send alert email: {mail_err}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
