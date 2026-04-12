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

    def split_cell_chunks(cell) -> list[str]:
        chunks: list[str] = []
        current: list[str] = []
        for child in cell.children:
            if getattr(child, "name", None) == "br":
                text = " ".join(" ".join(current).split())
                if text:
                    chunks.append(text)
                current = []
                continue
            text = child.get_text(" ", strip=True) if hasattr(child, "get_text") else str(child).strip()
            if text:
                current.append(text)

        text = " ".join(" ".join(current).split())
        if text:
            chunks.append(text)

        if not chunks:
            fallback = " ".join(cell.get_text(" ", strip=True).split())
            if fallback:
                chunks.append(fallback)
        return chunks

    def split_pitcher_chunks(cell) -> tuple[list[str], list[str | None]]:
        names: list[str] = []
        external_ids: list[str | None] = []
        current_name: list[str] = []
        current_id: str | None = None

        for child in cell.children:
            if getattr(child, "name", None) == "br":
                text = " ".join(" ".join(current_name).split())
                if text:
                    names.append(text)
                    external_ids.append(current_id)
                current_name = []
                current_id = None
                continue

            if getattr(child, "name", None) == "a":
                link_text = child.get_text(" ", strip=True)
                if link_text:
                    current_name.append(link_text)
                href = child.get("href", "")
                m = re.search(r"/id/(\d+)", href)
                if m:
                    current_id = m.group(1)
            else:
                text = child.get_text(" ", strip=True) if hasattr(child, "get_text") else str(child).strip()
                if text:
                    current_name.append(text)

        text = " ".join(" ".join(current_name).split())
        if text:
            names.append(text)
            external_ids.append(current_id)

        if not names:
            fallback = " ".join(cell.get_text(" ", strip=True).split())
            if fallback:
                names.append(fallback)
                external_ids.append(None)

        return names, external_ids

    def parse_matchup(matchup_text: str | None) -> tuple[str | None, str | None]:
        if not matchup_text:
            return None, None
        cleaned = " ".join(matchup_text.split()).upper()

        raw_tokens = re.findall(r"@?[A-Z]{2,3}|OFF", cleaned)
        if not raw_tokens:
            return None, None

        has_off = "OFF" in raw_tokens
        team_tokens = [token for token in raw_tokens if token != "OFF"]
        if not team_tokens:
            return None, "OFF" if has_off else None

        team_token = team_tokens[0].lstrip("@")
        opp_token = team_tokens[1].lstrip("@") if len(team_tokens) > 1 else None

        if not opp_token and has_off:
            opp_token = "OFF"

        return normalize_team_abbr(team_token), normalize_team_abbr(opp_token) if opp_token else None

    def is_header_value(value: str | None, blocked: set[str]) -> bool:
        if not value:
            return False
        normalized = " ".join(value.split()).upper()
        return normalized in blocked

    def looks_like_player_name(value: str | None) -> bool:
        if not value:
            return False
        text = " ".join(value.split())
        if not text:
            return False
        upper_text = text.upper()
        if upper_text in {"TEAM", "PITCHER"}:
            return False
        if not re.search(r"[A-Za-z]", text):
            return False
        if text == upper_text and " " not in text:
            return False
        return True

    heading = soup.find(string=re.compile(r"Next\s*10\s*days", re.IGNORECASE))
    if heading:
        heading_text = " ".join(str(heading).split())
        date_match = re.search(r"([A-Za-z]{3,9}\.?\s+\d{1,2}(?:\s*-\s*[A-Za-z]{3,9}\.?\s+\d{1,2})?)", heading_text)
        if date_match:
            forecaster_for_date = date_match.group(1)

    for tr in soup.select("table tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 3:
            continue

        texts = [" ".join(cell.get_text(" ", strip=True).split()) for cell in cells]
        if not texts or ("pitcher" in texts[0].lower() and "matchup" in " ".join(texts).lower()):
            continue

        pitcher_names, espn_ids = split_pitcher_chunks(cells[0])
        matchup_chunks = split_cell_chunks(cells[1])
        projection_chunks = split_cell_chunks(cells[2])
        if not pitcher_names:
            continue
        row_count = max(len(pitcher_names), len(matchup_chunks), len(projection_chunks))
        if row_count == 0:
            continue

        for idx in range(row_count):
            pitcher_name = pitcher_names[idx] if idx < len(pitcher_names) else None
            espn_player_id = espn_ids[idx] if idx < len(espn_ids) else None
            matchup_text = matchup_chunks[idx] if idx < len(matchup_chunks) else None
            projection_text = projection_chunks[idx] if idx < len(projection_chunks) else None

            team_abbr, opponent_team_abbr = parse_matchup(matchup_text)

            if not pitcher_name or pitcher_name.lower() == "pitcher":
                continue
            if is_header_value(pitcher_name, {"TEAM", "PITCHER"}):
                continue
            if is_header_value(matchup_text, {"DATE", "OPP"}):
                continue
            if is_header_value(projection_text, {"OPP", "T FPTS"}):
                continue
            if not espn_player_id and not looks_like_player_name(pitcher_name):
                continue

            rows.append(
                {
                    "source_name": SOURCE_NAME,
                    "espn_player_id": espn_player_id,
                    "pitcher_name": pitcher_name,
                    "team_abbr": team_abbr,
                    "opponent_team_abbr": opponent_team_abbr,
                    "matchup_text": matchup_text,
                    "projection_text": projection_text,
                    "raw_cells": texts,
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

    html = fetch_page(args.url)
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
