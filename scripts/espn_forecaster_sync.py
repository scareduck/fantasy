#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
from pathlib import Path

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

ESPN_URL = "https://www.espn.com/fantasy/baseball/story/_/id/31165100/fantasy-baseball-forecaster-probable-starting-pitcher-projections-matchups-daily-weekly-leagues"
SOURCE_NAME = "espn_forecaster"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync ESPN forecaster pitching rows into MariaDB.")
    parser.add_argument("--url", default=ESPN_URL, help="Forecaster URL to fetch")
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

    heading = soup.find(string=re.compile(r"(Next\s*10\s*days|Pitching\s*Matchups)", re.IGNORECASE))
    if heading:
        heading_text = " ".join(str(heading).split())
        date_match = re.search(r"([A-Za-z]{3,9}\.?\s+\d{1,2}(?:\s*-\s*[A-Za-z]{3,9}\.?\s+\d{1,2})?)", heading_text)
        if date_match:
            forecaster_for_date = date_match.group(1)

    def is_matchup_token(text: str) -> bool:
        token = text.strip().upper()
        return bool(re.fullmatch(r"(?:@[A-Z]{2,3}|[A-Z]{2,3}|OFF)", token))

    def extract_team_code(img_src: str | None) -> str | None:
        if not img_src:
            return None
        team_match = re.search(r"/teamlogos/mlb/\d+/([a-z]{2,3})\.(?:png|svg)", img_src, re.IGNORECASE)
        if not team_match:
            return None
        return normalize_team_abbr(team_match.group(1).upper())

    def split_cell_entries(cell) -> list[dict]:
        entries: list[dict] = []
        chunk: list = []
        for node in cell.children:
            if getattr(node, "name", None) == "br":
                entries.append(_entry_from_chunk(chunk))
                chunk = []
            else:
                chunk.append(node)
        if chunk:
            entries.append(_entry_from_chunk(chunk))
        return entries

    def _entry_from_chunk(chunk: list) -> dict:
        text_parts: list[str] = []
        player_id = None
        for part in chunk:
            part_text = ""
            if hasattr(part, "get_text"):
                part_text = part.get_text(" ", strip=True)
                if player_id is None:
                    anchor = part if getattr(part, "name", None) == "a" else part.find("a", href=True)
                    if anchor:
                        match = re.search(r"/player/_/id/(\d+)", anchor.get("href", ""))
                        if match:
                            player_id = match.group(1)
            else:
                part_text = str(part).strip()
            if part_text:
                text_parts.append(part_text)
        return {"text": " ".join(text_parts).strip(), "espn_player_id": player_id}

    def is_fpts_token(text: str) -> bool:
        return bool(re.fullmatch(r"\d+(?:\.\d+)?", text.strip()))

    def cell_score(cell) -> tuple[int, int, int]:
        entries = split_cell_entries(cell)
        matchup_count = sum(1 for item in entries if is_matchup_token(item["text"]))
        pitcher_count = sum(1 for item in entries if item.get("espn_player_id"))
        fpts_count = sum(1 for item in entries if is_fpts_token(item["text"]))
        return matchup_count, pitcher_count, fpts_count

    team_logo_imgs = soup.select("img[src*='/teamlogos/mlb/']")
    for logo in team_logo_imgs:
        team_abbr = extract_team_code(logo.get("src"))
        if not team_abbr:
            continue

        block = logo
        while block.parent is not None:
            parent = block.parent
            if len(parent.select("img[src*='/teamlogos/mlb/']")) > 1:
                break
            block = parent

        cells = block.find_all(["td", "th"])
        if len(cells) < 3:
            continue

        scored_cells = [(cell, *cell_score(cell)) for cell in cells]
        opponent_cell = max(scored_cells, key=lambda item: item[1])[0]
        pitcher_cell = max(scored_cells, key=lambda item: item[2])[0]
        fpts_cell = max(scored_cells, key=lambda item: item[3])[0]

        opponents = [item["text"].upper() for item in split_cell_entries(opponent_cell) if is_matchup_token(item["text"])]
        pitcher_entries = split_cell_entries(pitcher_cell)
        fpts_entries = [item["text"] for item in split_cell_entries(fpts_cell) if is_fpts_token(item["text"])]

        if len(opponents) < 5:
            continue

        max_rows = max(len(opponents), len(pitcher_entries))
        for idx in range(max_rows):
            matchup_token = opponents[idx] if idx < len(opponents) else ""
            if matchup_token == "OFF":
                continue

            pitcher_name = None
            espn_player_id = None
            if idx < len(pitcher_entries):
                pitcher_name = pitcher_entries[idx]["text"] or None
                espn_player_id = pitcher_entries[idx]["espn_player_id"]
            if not pitcher_name:
                continue

            if matchup_token.startswith("@"):
                opponent_team_abbr = normalize_team_abbr(matchup_token[1:])
            else:
                opponent_team_abbr = normalize_team_abbr(matchup_token)

            projection_text = fpts_entries[idx] if idx < len(fpts_entries) else None

            rows.append(
                {
                    "source_name": SOURCE_NAME,
                    "espn_player_id": espn_player_id,
                    "pitcher_name": pitcher_name,
                    "team_abbr": team_abbr,
                    "opponent_team_abbr": opponent_team_abbr,
                    "matchup_text": matchup_token,
                    "projection_text": projection_text,
                    "raw_cells": [" ".join(text.split()) for text in block.stripped_strings],
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
