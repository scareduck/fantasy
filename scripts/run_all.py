#!/usr/bin/env python3
"""Run the full Fantasy Baseball sync pipeline in sequence."""
from __future__ import annotations

import argparse
import sys

from fantasy.notify import send_alert
from fantasy.yahoo_auth import InteractiveAuthRequired
from scripts.batter_sync import main as batter_main
from scripts.espn_forecaster_sync import main as espn_main
from scripts.fa_il_pitcher_report import main as il_pitchers_main
from scripts.mlb_schedule_sync import main as mlb_schedule_main
from scripts.pitcher_report import main as report_main
from scripts.pitcher_stats_sync import run as pitcher_stats_run, parse_args as pitcher_stats_parse_args
from scripts.waiver_report import main as waiver_main
from scripts.yahoo_sync import parse_args, run as sync_run


def parse_run_all_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full Fantasy Baseball sync pipeline.")
    parser.add_argument("--send-report", action="store_true", help="Email pitcher reports to all recipients.")
    parser.add_argument(
        "--report-recipients",
        nargs="+",
        metavar="NAME",
        help="Limit report recipients (default: all). E.g. --report-recipients rob helen",
    )
    parser.add_argument("--analyze-il", action="store_true", help="Run FA IL pitcher AI analysis (has API cost).")
    return parser.parse_args()


def main() -> int:
    run_args = parse_run_all_args()

    try:
        print("=== Step 1: Yahoo sync ===")
        args = parse_args([])
        args.all_rosters = True
        rc = sync_run(args)
        if rc:
            print(f"Yahoo sync failed (exit {rc}), aborting.", file=sys.stderr)
            return rc

        print("\n=== Step 2: Batter season stats ===")
        rc = batter_main([])
        if rc:
            print(f"Batter stats sync failed (exit {rc}), aborting.", file=sys.stderr)
            return rc

        print("\n=== Step 3: Pitcher season stats ===")
        rc = pitcher_stats_run(pitcher_stats_parse_args([]))
        if rc:
            print(f"Pitcher stats sync failed (exit {rc}), aborting.", file=sys.stderr)
            return rc

        print("\n=== Step 4: ESPN forecaster sync ===")
        rc = espn_main([])
        if rc:
            print(f"ESPN sync failed (exit {rc}), aborting.", file=sys.stderr)
            return rc

        print("\n=== Step 5: MLB schedule sync ===")
        rc = mlb_schedule_main([])
        if rc:
            print(f"MLB schedule sync failed (exit {rc}), aborting.", file=sys.stderr)
            return rc

        if run_args.analyze_il:
            print("\n=== Step 6: FA IL pitcher analysis ===")
            rc = il_pitchers_main([])
            if rc:
                print(f"IL pitcher analysis failed (exit {rc}).", file=sys.stderr)
                return rc
        else:
            print("\n=== Step 6: Skipping IL pitcher analysis (--analyze-il not set) ===")

        if run_args.send_report:
            print("\n=== Step 7: Pitcher reports ===")
            report_argv: list[str] = ["--email"]
            if run_args.report_recipients:
                report_argv += ["--recipients"] + run_args.report_recipients
            rc = report_main(report_argv)
            if rc:
                print(f"Report failed (exit {rc}).", file=sys.stderr)
                return rc

            print("\n=== Step 8: Waiver wire report ===")
            waiver_argv: list[str] = ["--email"]
            if run_args.report_recipients:
                waiver_argv += ["--recipients"] + run_args.report_recipients
            rc = waiver_main(waiver_argv)
            if rc:
                print(f"Waiver report failed (exit {rc}).", file=sys.stderr)
                return rc
        else:
            print("\n=== Steps 7-8: Skipping reports (--send-report not set) ===")

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
