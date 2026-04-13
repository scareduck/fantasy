#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import requests
from bs4 import BeautifulSoup

from fantasy.config import load_db_sync_settings
from fantasy.db import (
    connect,
    insert_espn_forecaster_snapshot,
    load_external_player_map,
    load_pitcher_name_team_maps,
)
from fantasy.espn_forecaster import correlate_forecaster_row, normalize_team_abbr
from fantasy.utils import format_snapshot_timestamp, utc_now

ESPN_FALLBACK_URL = "https://www.espn.com/fantasy/baseball/story/_/id/31165100/fantasy-baseball-forecaster-probable-starting-pitcher-projections-matchups-daily-weekly-leagues"
ESPN_INDEX_URL = "https://www.espn.com/fantasy/baseball/"
SOURCE_NAME = "espn_forecaster"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

_FORECASTER_PATTERNS = [
    re.compile(r"forecaster.*pitcher projections.*next \d+ days", re.I),
    re.compile(r"forecaster.*starting pitcher.*week \d+", re.I),
    re.compile(r"fantasy baseball forecaster.*week \d+", re.I),
    re.compile(r"forecaster.*pitcher projections", re.I),
]


def discover_forecaster_url() -> str:
    """
    Scrape the ESPN fantasy baseball index page for the current forecaster link.
    Falls back to ESPN_FALLBACK_URL if discovery fails.
    No hardcoded story IDs — matches on link text patterns only.
    """
    try:
        resp = requests.get(ESPN_INDEX_URL, timeout=15, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for anchor in soup.find_all("a", href=True):
            text = anchor.get_text(strip=True)
            href = anchor["href"]
            if "espn.com/fantasy/baseball/story" not in href:
                if not href.startswith("http"):
                    href = "https://www.espn.com" + href
                if "espn.com/fantasy/baseball/story" not in href:
                    continue
            for pat in _FORECASTER_PATTERNS:
                if pat.search(text):
                    return href
    except Exception:
        pass
    return ESPN_FALLBACK_URL


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync ESPN forecaster pitching rows into MariaDB.")
    parser.add_argument("--url", default=None, help="Forecaster URL to fetch (default: auto-discover)")
    parser.add_argument("--dry-run", action="store_true", help="Parse and write CSV outputs without DB inserts")
    return parser.parse_args()


def fetch_page(url: str) -> str:
    response = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.text


def parse_espn_forecaster_rows(html: str) -> tuple[list[dict], str | None]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    forecaster_for_date = None

    # Extract date range from headings (e.g. "April 13-19" or "April 13 - April 19")
    for tag in soup.find_all(re.compile(r"h[1-5]")):
        text = tag.get_text(" ", strip=True)
        m = re.search(
            r"([A-Za-z]{3,9}\.?\s+\d{1,2}\s*[-\u2013]\s*(?:[A-Za-z]{3,9}\.?\s+)?\d{1,2})",
            text,
        )
        if m:
            forecaster_for_date = m.group(1)
            break

    # Find the pitcher rankings table by its header row
    pitcher_table = None
    for table in soup.select("table"):
        header_row = table.select_one("tr")
        if not header_row:
            continue
        headers = [c.get_text(strip=True).upper() for c in header_row.find_all(["th", "td"])]
        if "PITCHER" in headers and "TEAM" in headers:
            pitcher_table = table
            break

    if pitcher_table is None:
        return rows, forecaster_for_date

    header_row = pitcher_table.select_one("tr")
    headers = [c.get_text(strip=True).upper() for c in header_row.find_all(["th", "td"])]
    try:
        pitcher_col = headers.index("PITCHER")
        team_col = headers.index("TEAM")
    except ValueError:
        return rows, forecaster_for_date

    start_cols = [i for i, h in enumerate(headers) if "START" in h]
    start1_col = start_cols[0] if len(start_cols) > 0 else None
    start2_col = start_cols[1] if len(start_cols) > 1 else None

    for tr in pitcher_table.select("tr")[1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) <= pitcher_col:
            continue

        pitcher_cell = cells[pitcher_col]
        pitcher_name = " ".join(pitcher_cell.get_text(" ", strip=True).split())
        if not pitcher_name or pitcher_name.upper() == "PITCHER":
            continue

        anchor = pitcher_cell.find("a", href=True)
        espn_player_id = None
        if anchor:
            m = re.search(r"/id/(\d+)", anchor["href"])
            if m:
                espn_player_id = m.group(1)

        team_text = cells[team_col].get_text(strip=True) if len(cells) > team_col else None
        team_abbr = normalize_team_abbr(team_text) if team_text else None

        matchup_text = (
            " ".join(cells[start1_col].get_text(" ", strip=True).split())
            if start1_col is not None and len(cells) > start1_col
            else None
        )
        projection_text = (
            " ".join(cells[start2_col].get_text(" ", strip=True).split())
            if start2_col is not None and len(cells) > start2_col
            else None
        )

        # Opponent is the team abbreviation after the hyphen in "Tue 4/14-@SD (King)"
        opponent_team_abbr = None
        if matchup_text:
            opp_m = re.search(r"-@?([A-Z]{2,4})\b", matchup_text.upper())
            if opp_m:
                opponent_team_abbr = normalize_team_abbr(opp_m.group(1))

        rows.append(
            {
                "source_name": SOURCE_NAME,
                "espn_player_id": espn_player_id,
                "pitcher_name": pitcher_name,
                "team_abbr": team_abbr,
                "opponent_team_abbr": opponent_team_abbr,
                "matchup_text": matchup_text,
                "projection_text": projection_text,
                "raw_cells": [" ".join(c.get_text(" ", strip=True).split()) for c in cells],
            }
        )

    return rows, forecaster_for_date


def write_rows_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "captured_at_utc",
        "forecaster_for_date",
        "espn_player_id",
        "pitcher_name",
        "team_abbr",
        "opponent_team_abbr",
        "matchup_text",
        "projection_text",
        "player_id",
        "match_method",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    if args.dry_run:
        settings = SimpleNamespace(
            snapshot_dir=Path(os.getenv("SNAPSHOT_DIR", "snapshots")).expanduser(),
            local_timezone=os.getenv("LOCAL_TIMEZONE", "America/Chicago").strip() or "America/Chicago",
        )
    else:
        settings = load_db_sync_settings()
    captured_at = utc_now()
    ts = format_snapshot_timestamp(captured_at, settings.local_timezone)

    html = fetch_page(args.url or discover_forecaster_url())
    rows, forecaster_for_date = parse_espn_forecaster_rows(html)
    if not rows:
        raise SystemExit("No forecaster rows were parsed from the ESPN page.")

    conn = connect(settings) if not args.dry_run else None
    try:
        if conn is not None:
            explicit_map = load_external_player_map(conn, SOURCE_NAME)
            full_map, ascii_map = load_pitcher_name_team_maps(conn)
        else:
            explicit_map = {}
            full_map, ascii_map = {}, {}

        snapshot_rows: list[dict] = []
        unresolved_rows: list[dict] = []

        for row in rows:
            match = correlate_forecaster_row(row, explicit_map, full_map, ascii_map)
            snapshot_row = {
                "captured_at_utc": captured_at.isoformat(),
                "forecaster_for_date": forecaster_for_date,
                "espn_player_id": row.get("espn_player_id"),
                "pitcher_name": row.get("pitcher_name"),
                "team_abbr": row.get("team_abbr"),
                "opponent_team_abbr": row.get("opponent_team_abbr"),
                "matchup_text": row.get("matchup_text"),
                "projection_text": row.get("projection_text"),
                "player_id": match.player_id,
                "match_method": match.method,
            }
            snapshot_rows.append(snapshot_row)

            if match.player_id is None:
                unresolved_rows.append(snapshot_row)

            if conn is not None:
                insert_espn_forecaster_snapshot(
                    conn,
                    source_name=SOURCE_NAME,
                    captured_at_utc=captured_at.replace(tzinfo=None),
                    forecaster_for_date=forecaster_for_date,
                    espn_player_id=row.get("espn_player_id"),
                    pitcher_name=row.get("pitcher_name") or "",
                    team_abbr=row.get("team_abbr"),
                    opponent_team_abbr=row.get("opponent_team_abbr"),
                    matchup_text=row.get("matchup_text"),
                    projection_text=row.get("projection_text"),
                    player_id=match.player_id,
                    match_method=match.method,
                    raw_row_payload=row,
                )

        snapshots_path = settings.snapshot_dir / f"espn_forecaster_{ts}.csv"
        unresolved_path = settings.snapshot_dir / f"espn_forecaster_unresolved_{ts}.csv"
        write_rows_csv(snapshots_path, snapshot_rows)
        write_rows_csv(unresolved_path, unresolved_rows)

        if conn is not None:
            conn.commit()

        print(f"Parsed {len(rows)} ESPN rows")
        print(f"Matched {len(rows) - len(unresolved_rows)} rows")
        print(f"Unresolved {len(unresolved_rows)} rows")
        print(f"Wrote snapshot CSV: {snapshots_path}")
        print(f"Wrote unresolved CSV: {unresolved_path}")
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
