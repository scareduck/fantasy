#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path

from fantasy_baseball.config import load_settings
from fantasy_baseball.db import connect, resolve_player_id
from fantasy_baseball.utils import parse_bool



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import pitcher enrichment data from CSV files.")
    parser.add_argument("--probables-csv", help="CSV file containing probable starters data")
    parser.add_argument("--projections-csv", help="CSV file containing projection data")
    parser.add_argument("--notes-csv", help="CSV file containing tags/notes data")
    return parser.parse_args()



def read_rows(path: str) -> list[dict]:
    with Path(path).open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))



def resolve(conn, row: dict) -> int:
    player_id = resolve_player_id(
        conn,
        yahoo_player_key=(row.get("yahoo_player_key") or "").strip() or None,
        player_name=(row.get("player_name") or "").strip() or None,
        editorial_team_abbr=(row.get("editorial_team_abbr") or "").strip() or None,
    )
    if player_id is None:
        raise RuntimeError(
            "Could not resolve player row. Provide yahoo_player_key or (player_name + editorial_team_abbr). "
            f"Row: {row}"
        )
    return player_id



def import_probables(conn, csv_path: str) -> int:
    rows = read_rows(csv_path)
    cur = conn.cursor()
    imported = 0
    for row in rows:
        player_id = resolve(conn, row)
        game_time_local = (row.get("game_time_local") or "").strip() or None
        cur.execute(
            """
            INSERT INTO probable_start (
                player_id,
                source_name,
                start_date,
                opponent_team_abbr,
                is_home,
                park,
                role_code,
                game_time_local,
                notes,
                captured_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP())
            ON DUPLICATE KEY UPDATE
                opponent_team_abbr = VALUES(opponent_team_abbr),
                is_home = VALUES(is_home),
                park = VALUES(park),
                role_code = VALUES(role_code),
                game_time_local = VALUES(game_time_local),
                notes = VALUES(notes),
                captured_at_utc = UTC_TIMESTAMP()
            """,
            (
                player_id,
                (row.get("source") or "manual").strip() or "manual",
                row["start_date"].strip(),
                (row.get("opponent_team_abbr") or "").strip() or None,
                parse_bool(row.get("is_home")),
                (row.get("park") or "").strip() or None,
                (row.get("role") or "").strip() or None,
                game_time_local,
                (row.get("notes") or "").strip() or None,
            ),
        )
        imported += 1
    return imported



def to_decimal(value: str | None) -> float | None:
    raw = (value or "").strip()
    if not raw:
        return None
    return float(raw)



def import_projections(conn, csv_path: str) -> int:
    rows = read_rows(csv_path)
    cur = conn.cursor()
    imported = 0
    for row in rows:
        player_id = resolve(conn, row)
        cur.execute(
            """
            INSERT INTO projection (
                player_id,
                source_name,
                projection_date,
                innings,
                wins,
                strikeouts,
                era,
                whip,
                sv_holds,
                espn_fpts,
                opponent_team_abbr,
                park,
                notes,
                captured_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP())
            ON DUPLICATE KEY UPDATE
                innings = VALUES(innings),
                wins = VALUES(wins),
                strikeouts = VALUES(strikeouts),
                era = VALUES(era),
                whip = VALUES(whip),
                sv_holds = VALUES(sv_holds),
                espn_fpts = VALUES(espn_fpts),
                opponent_team_abbr = VALUES(opponent_team_abbr),
                park = VALUES(park),
                notes = VALUES(notes),
                captured_at_utc = UTC_TIMESTAMP()
            """,
            (
                player_id,
                (row.get("source") or "manual").strip() or "manual",
                row["projection_date"].strip(),
                to_decimal(row.get("innings")),
                to_decimal(row.get("wins")),
                to_decimal(row.get("strikeouts")),
                to_decimal(row.get("era")),
                to_decimal(row.get("whip")),
                to_decimal(row.get("sv_holds")),
                to_decimal(row.get("espn_fpts")),
                (row.get("opponent_team_abbr") or "").strip() or None,
                (row.get("park") or "").strip() or None,
                (row.get("notes") or "").strip() or None,
            ),
        )
        imported += 1
    return imported



def import_notes(conn, csv_path: str) -> int:
    rows = read_rows(csv_path)
    cur = conn.cursor()
    imported = 0
    for row in rows:
        player_id = resolve(conn, row)
        cur.execute(
            """
            INSERT INTO stream_note (
                player_id,
                tag,
                note_text,
                source_name,
                is_active
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                player_id,
                row["tag"].strip(),
                (row.get("note_text") or "").strip() or None,
                (row.get("source") or "manual").strip() or "manual",
                1 if parse_bool(row.get("is_active") or "1") else 0,
            ),
        )
        imported += 1
    return imported



def main() -> int:
    args = parse_args()
    if not any([args.probables_csv, args.projections_csv, args.notes_csv]):
        raise SystemExit("Provide at least one of --probables-csv, --projections-csv, or --notes-csv")

    settings = load_settings()
    conn = connect(settings)

    try:
        total = 0
        if args.probables_csv:
            count = import_probables(conn, args.probables_csv)
            print(f"Imported probable starters: {count}")
            total += count
        if args.projections_csv:
            count = import_projections(conn, args.projections_csv)
            print(f"Imported projections:      {count}")
            total += count
        if args.notes_csv:
            count = import_notes(conn, args.notes_csv)
            print(f"Imported notes/tags:      {count}")
            total += count
        conn.commit()
        print(f"Committed enrichment rows: {total}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
