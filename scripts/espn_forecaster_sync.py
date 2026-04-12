#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
from pathlib import Path

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

ESPN_URL = "https://www.espn.com/fantasy/baseball/story/_/id/31165100/fantasy-baseball-forecaster-probable-starting-pitcher-projections-matchups-daily-weekly-leagues"
SOURCE_NAME = "espn_forecaster"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync ESPN forecaster pitching rows into MariaDB.")
    parser.add_argument("--url", default=ESPN_URL, help="Forecaster URL to fetch")
    parser.add_argument("--dry-run", action="store_true", help="Parse and write CSV outputs without DB inserts")
    return parser.parse_args()


def fetch_page(url: str) -> str:
    import requests

    response = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.text


def parse_espn_forecaster_rows(html: str) -> tuple[list[dict], str | None]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    forecaster_for_date = None

    heading = soup.find(string=re.compile(r"(Next\s*10\s*days|Pitching\s*Matchups)", re.IGNORECASE))
    if heading:
        heading_text = " ".join(str(heading).split())
        date_match = re.search(r"([A-Za-z]{3,9}\.?\s+\d{1,2}(?:\s*-\s*[A-Za-z]{3,9}\.?\s+\d{1,2})?)", heading_text)
        if date_match:
            forecaster_for_date = date_match.group(1)

    def is_matchup_token(text: str) -> bool:
        return bool(re.fullmatch(r"(?:@[A-Z]{2,3}|[A-Z]{2,3}|OFF)", text.strip().upper()))

    def split_cell_entries(cell) -> list[dict]:
        entries: list[dict] = []
        parts = re.split(r"<br\s*/?>", cell.decode_contents(), flags=re.IGNORECASE)
        for part in parts:
            fragment = BeautifulSoup(part, "html.parser")
            text = fragment.get_text(" ", strip=True)
            if not text:
                continue
            anchor = fragment.find("a", href=True)
            espn_player_id = None
            if anchor:
                match = re.search(r"/player/_/id/(\d+)", anchor.get("href", ""))
                if match:
                    espn_player_id = match.group(1)
            entries.append({"text": text, "espn_player_id": espn_player_id})
        return entries

    def normalize_matchup_token(text: str) -> str:
        return text.strip().upper().replace(".", "")

    def normalize_team_token(text: str) -> str:
        token = normalize_matchup_token(text)
        if token == "OFF":
            return token
        return normalize_team_abbr(token[1:] if token.startswith("@") else token)

    def normalize_projection(text: str) -> str | None:
        cleaned = text.strip()
        if re.fullmatch(r"\d+(?:\.\d+)?", cleaned):
            return cleaned
        return None

    for tr in soup.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 4:
            continue

        team_entries = [normalize_matchup_token(item["text"]) for item in split_cell_entries(cells[0])]
        opp_entries = [normalize_matchup_token(item["text"]) for item in split_cell_entries(cells[1])]
        pitcher_entries = split_cell_entries(cells[2])
        fpts_entries = [normalize_projection(item["text"]) for item in split_cell_entries(cells[3])]

        if not team_entries or not any(is_matchup_token(token) for token in team_entries):
            continue

        max_rows = max(len(team_entries), len(opp_entries), len(pitcher_entries), len(fpts_entries))
        for idx in range(max_rows):
            team_token = team_entries[idx] if idx < len(team_entries) else ""
            opp_token = opp_entries[idx] if idx < len(opp_entries) else ""
            pitcher = pitcher_entries[idx] if idx < len(pitcher_entries) else {"text": "", "espn_player_id": None}
            fpts = fpts_entries[idx] if idx < len(fpts_entries) else None

            if not is_matchup_token(team_token):
                continue
            if opp_token and not is_matchup_token(opp_token):
                continue
            if team_token == "OFF" or opp_token == "OFF":
                continue

            pitcher_name = pitcher["text"].strip()
            if not pitcher_name:
                continue

            team_abbr = normalize_team_token(team_token)
            opponent_team_abbr = normalize_team_token(opp_token) if opp_token else ""
            if not team_abbr or not opponent_team_abbr:
                continue

            rows.append(
                {
                    "source_name": SOURCE_NAME,
                    "espn_player_id": pitcher.get("espn_player_id"),
                    "pitcher_name": pitcher_name,
                    "team_abbr": team_abbr,
                    "opponent_team_abbr": opponent_team_abbr,
                    "matchup_text": team_token,
                    "projection_text": fpts,
                    "raw_cells": [" ".join(text.split()) for text in tr.stripped_strings],
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
    captured_at = utc_now()
    local_timezone = os.getenv("LOCAL_TIMEZONE", "America/Chicago").strip() or "America/Chicago"
    ts = format_snapshot_timestamp(captured_at, local_timezone)
    snapshot_dir = Path(os.getenv("SNAPSHOT_DIR", "snapshots")).expanduser()

    html = fetch_page(args.url)
    rows, forecaster_for_date = parse_espn_forecaster_rows(html)
    if not rows:
        raise SystemExit("No forecaster rows were parsed from the ESPN page.")

    conn = None
    explicit_map: dict[tuple[str, str], int] = {}
    full_map: dict[tuple[str, str], list[int]] = {}
    ascii_map: dict[tuple[str, str], list[int]] = {}
    if not args.dry_run:
        settings = load_db_sync_settings()
        snapshot_dir = settings.snapshot_dir
        conn = connect(settings)
        explicit_map = load_external_player_map(conn, SOURCE_NAME)
        full_map, ascii_map = load_pitcher_name_team_maps(conn)

    try:
        snapshot_rows: list[dict] = []
        unresolved_rows: list[dict] = []

        for row in rows:
            if args.dry_run:
                player_id = None
                match_method = "dry_run_unresolved"
            else:
                match = correlate_forecaster_row(row, explicit_map, full_map, ascii_map)
                player_id = match.player_id
                match_method = match.method

            snapshot_row = {
                "captured_at_utc": captured_at.isoformat(),
                "forecaster_for_date": forecaster_for_date,
                "espn_player_id": row.get("espn_player_id"),
                "pitcher_name": row.get("pitcher_name"),
                "team_abbr": row.get("team_abbr"),
                "opponent_team_abbr": row.get("opponent_team_abbr"),
                "matchup_text": row.get("matchup_text"),
                "projection_text": row.get("projection_text"),
                "player_id": player_id,
                "match_method": match_method,
            }
            snapshot_rows.append(snapshot_row)

            if player_id is None:
                unresolved_rows.append(snapshot_row)

            if not args.dry_run:
                assert conn is not None
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
                    player_id=player_id,
                    match_method=match_method,
                    raw_row_payload=row,
                )

        snapshots_path = snapshot_dir / f"espn_forecaster_{ts}.csv"
        unresolved_path = snapshot_dir / f"espn_forecaster_unresolved_{ts}.csv"
        write_rows_csv(snapshots_path, snapshot_rows)
        write_rows_csv(unresolved_path, unresolved_rows)

        if not args.dry_run:
            assert conn is not None
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
