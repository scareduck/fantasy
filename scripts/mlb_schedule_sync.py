#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from urllib.request import urlopen
from urllib.error import URLError

from fantasy.config import load_db_sync_settings
from fantasy.db import (
    connect,
    get_player_ids_missing_throws,
    load_pitcher_name_team_maps,
    update_player_throws,
    upsert_mlb_game,
)
from fantasy.espn_forecaster import correlate_forecaster_row, normalize_team_abbr

MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
MLB_PEOPLE_URL   = "https://statsapi.mlb.com/api/v1/people"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync MLB schedule and probable starters into MariaDB.")
    parser.add_argument("--days", type=int, default=14,
                        help="Number of days ahead to fetch (default: 14)")
    parser.add_argument("--date", default=None,
                        help="Fetch a single date only (YYYY-MM-DD)")
    return parser.parse_args(argv)


def fetch_schedule(start_date: str, end_date: str) -> list[dict]:
    url = (
        f"{MLB_SCHEDULE_URL}?sportId=1"
        f"&startDate={start_date}&endDate={end_date}"
        f"&hydrate=probablePitcher,team"
    )
    with urlopen(url, timeout=30) as resp:
        data = json.load(resp)
    games: list[dict] = []
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            if game.get("gameType") == "R":
                games.append(game)
    return games


def extract_game(game: dict) -> dict:
    away = game["teams"]["away"]
    home = game["teams"]["home"]

    away_abbr = normalize_team_abbr(away["team"].get("abbreviation", ""))
    home_abbr = normalize_team_abbr(home["team"].get("abbreviation", ""))

    away_pitcher = away.get("probablePitcher") or {}
    home_pitcher = home.get("probablePitcher") or {}

    raw_dt = game.get("gameDate", "")
    game_datetime_utc = raw_dt[:19].replace("T", " ") if raw_dt else None

    return {
        "game_pk":          game["gamePk"],
        "game_date":        game["officialDate"],
        "game_datetime_utc": game_datetime_utc,
        "home_team_abbr":   home_abbr,
        "away_team_abbr":   away_abbr,
        "home_pitcher_name":   home_pitcher.get("fullName") or None,
        "home_pitcher_mlb_id": home_pitcher.get("id") or None,
        "away_pitcher_name":   away_pitcher.get("fullName") or None,
        "away_pitcher_mlb_id": away_pitcher.get("id") or None,
    }


def fetch_pitch_hands(mlb_ids: list[int]) -> dict[int, str]:
    """Return a map of MLB person id -> pitching hand ('L' or 'R')."""
    result: dict[int, str] = {}
    chunk_size = 50
    for i in range(0, len(mlb_ids), chunk_size):
        chunk = mlb_ids[i:i + chunk_size]
        url = f"{MLB_PEOPLE_URL}?personIds={','.join(str(i) for i in chunk)}"
        with urlopen(url, timeout=30) as resp:
            data = json.load(resp)
        for person in data.get("people", []):
            hand = (person.get("pitchHand") or {}).get("code")
            if hand:
                result[person["id"]] = hand
    return result


def resolve_pitcher(
    name: str | None,
    team_abbr: str | None,
    full_map: dict,
    ascii_map: dict,
) -> int | None:
    if not name or not team_abbr:
        return None
    row = {"pitcher_name": name, "team_abbr": team_abbr,
           "espn_player_id": None, "source_name": "mlb_schedule"}
    result = correlate_forecaster_row(row, {}, full_map, ascii_map)
    return result.player_id


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_db_sync_settings()
    conn = connect(settings)

    try:
        if args.date:
            start = end = args.date
        else:
            today = date.today()
            start = today.isoformat()
            end = (today + timedelta(days=args.days - 1)).isoformat()

        print(f"Fetching MLB schedule {start} to {end}...")
        try:
            games = fetch_schedule(start, end)
        except URLError as exc:
            raise SystemExit(f"Failed to fetch MLB schedule: {exc}") from exc

        print(f"Found {len(games)} regular-season games")

        full_map, ascii_map = load_pitcher_name_team_maps(conn)

        upserted = 0
        pitcher_mlb_id_by_player_id: dict[int, int] = {}
        for game in games:
            row = extract_game(game)
            row["home_pitcher_player_id"] = resolve_pitcher(
                row["home_pitcher_name"], row["home_team_abbr"], full_map, ascii_map
            )
            row["away_pitcher_player_id"] = resolve_pitcher(
                row["away_pitcher_name"], row["away_team_abbr"], full_map, ascii_map
            )
            for side in ("home", "away"):
                pid = row[f"{side}_pitcher_player_id"]
                mlb_id = row[f"{side}_pitcher_mlb_id"]
                if pid and mlb_id:
                    pitcher_mlb_id_by_player_id[pid] = mlb_id
            upsert_mlb_game(conn, **row)
            upserted += 1

        conn.commit()
        print(f"Upserted {upserted} games")

        missing = get_player_ids_missing_throws(conn, list(pitcher_mlb_id_by_player_id))
        if missing:
            print(f"Fetching pitching hand for {len(missing)} pitcher(s)...")
            hands = fetch_pitch_hands([pitcher_mlb_id_by_player_id[pid] for pid in missing])
            updated = 0
            for pid in missing:
                hand = hands.get(pitcher_mlb_id_by_player_id[pid])
                if hand:
                    update_player_throws(conn, pid, hand)
                    updated += 1
            conn.commit()
            print(f"Cached throws for {updated} pitcher(s)")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
