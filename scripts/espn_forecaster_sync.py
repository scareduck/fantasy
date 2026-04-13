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

def discover_forecaster_url() -> str:
    """
    Try the known evergreen forecaster URL first (has FPTS data).
    Falls back to ESPN_FALLBACK_URL if the primary is unreachable.
    """
    try:
        resp = requests.get(ESPN_FALLBACK_URL, timeout=15, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for table in soup.select("table"):
            header = table.select_one("tr")
            if header:
                headers = [c.get_text(strip=True).upper() for c in header.find_all(["th", "td"])]
                if "FPTS" in headers:
                    return ESPN_FALLBACK_URL
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


def _br_chunks(cell) -> list[str]:
    """Split a table cell's content by <br> tags, returning stripped text per slot."""
    chunks: list[str] = []
    current: list[str] = []
    container = cell.find("div") or cell
    for child in container.children:
        if getattr(child, "name", None) == "br":
            chunks.append(" ".join(current).strip())
            current = []
        elif hasattr(child, "get_text"):
            text = child.get_text(" ", strip=True)
            if text:
                current.append(text)
        else:
            text = str(child).strip()
            if text:
                current.append(text)
    if current:
        chunks.append(" ".join(current).strip())
    return chunks


def _pitcher_br_chunks(cell) -> list[tuple[str, str | None]]:
    """Split pitcher cell by <br> tags, returning (name, espn_id) per slot."""
    pairs: list[tuple[str, str | None]] = []
    current_name: list[str] = []
    current_id: str | None = None
    container = cell.find("div") or cell
    for child in container.children:
        if getattr(child, "name", None) == "br":
            pairs.append((" ".join(current_name).strip(), current_id))
            current_name = []
            current_id = None
        elif getattr(child, "name", None) == "a":
            href = child.get("href", "")
            m = re.search(r"/id/(\d+)", href)
            if m:
                current_id = m.group(1)
            current_name.append(child.get_text(" ", strip=True))
        elif hasattr(child, "get_text"):
            text = child.get_text(" ", strip=True)
            if text:
                current_name.append(text)
        else:
            text = str(child).strip()
            if text:
                current_name.append(text)
    pairs.append((" ".join(current_name).strip(), current_id))
    return pairs


def parse_espn_forecaster_rows(html: str) -> tuple[list[dict], str | None]:
    """
    Parse the ESPN forecaster page (TEAM/DATE/OPP/PITCHER/T/FPTS format).
    Each table row is a team block; cells are BR-separated, one entry per game day.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    forecaster_for_date = None

    # Extract date range from headings
    for tag in soup.find_all(re.compile(r"h[1-5]")):
        text = tag.get_text(" ", strip=True)
        m = re.search(
            r"([A-Za-z]{3,9}\.?\s+\d{1,2}\s*[-\u2013]\s*(?:[A-Za-z]{3,9}\.?\s+)?\d{1,2})",
            text,
        )
        if m:
            forecaster_for_date = m.group(1)
            break

    # Find the FPTS forecaster table
    fpts_table = None
    for table in soup.select("table"):
        header = table.select_one("tr")
        if not header:
            continue
        headers = [c.get_text(strip=True).upper() for c in header.find_all(["th", "td"])]
        if "FPTS" in headers and "PITCHER" in headers:
            fpts_table = table
            break

    if fpts_table is None:
        return rows, forecaster_for_date

    header_row = fpts_table.select_one("tr")
    headers = [c.get_text(strip=True).upper() for c in header_row.find_all(["th", "td"])]
    try:
        date_col = headers.index("DATE")
        opp_col = headers.index("OPP")
        pitcher_col = headers.index("PITCHER")
        fpts_col = headers.index("FPTS")
    except ValueError:
        return rows, forecaster_for_date

    for tr in fpts_table.select("tr")[1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) <= fpts_col:
            continue

        # Team from logo image src: .../mlb/500/ari.png -> ARI
        team_abbr = None
        img = cells[0].find("img")
        if img:
            m = re.search(r"/mlb/500/(\w+)\.png", img.get("src", ""))
            if m:
                team_abbr = normalize_team_abbr(m.group(1).upper())
        if not team_abbr:
            continue

        pitcher_pairs = _pitcher_br_chunks(cells[pitcher_col])
        date_chunks = _br_chunks(cells[date_col])
        opp_chunks = _br_chunks(cells[opp_col])
        fpts_chunks = _br_chunks(cells[fpts_col])

        for idx, (pitcher_name, espn_player_id) in enumerate(pitcher_pairs):
            if not pitcher_name or pitcher_name.upper() == "TBD":
                continue

            raw_date = date_chunks[idx] if idx < len(date_chunks) else ""
            opp_text = opp_chunks[idx] if idx < len(opp_chunks) else ""
            fpts_text = fpts_chunks[idx] if idx < len(fpts_chunks) else ""

            if not opp_text or opp_text.upper() == "OFF":
                opponent_team_abbr = None
                matchup_text = None
            else:
                opponent_team_abbr = normalize_team_abbr(opp_text.lstrip("@"))
                date_str = raw_date.replace(",", "").strip()
                matchup_text = f"{date_str}-{opp_text}" if date_str else opp_text

            rows.append(
                {
                    "source_name": SOURCE_NAME,
                    "espn_player_id": espn_player_id,
                    "pitcher_name": pitcher_name,
                    "team_abbr": team_abbr,
                    "opponent_team_abbr": opponent_team_abbr,
                    "matchup_text": matchup_text,
                    "projection_text": fpts_text or None,
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
            snapshot_dir=Path(os.getenv("SNAPSHOT_DIR", Path(__file__).parent.parent / "snapshots")),
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
