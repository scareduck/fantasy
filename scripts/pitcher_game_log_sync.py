#!/usr/bin/env python3
"""Sync per-date pitcher SV+H stats into pitcher_game_log.

Default: yesterday.  Use --since YYYY-MM-DD for a backfill.

Dates that already have rows are skipped unless --force is given.
Note: a date where no pitcher earned SV+H will have 0 rows and will
therefore be re-synced on a subsequent --force or --since run; this is
harmless since there is nothing to insert.
"""
from __future__ import annotations

import argparse
import time
from datetime import date, timedelta

from fantasy.config import load_settings
from fantasy.db import (
    connect,
    game_log_date_exists,
    insert_pitcher_game_log,
    load_stat_id_map,
    upsert_league,
    upsert_league_stat_categories,
    upsert_player,
)
from fantasy.utils import utc_now
from fantasy.yahoo_client import YahooFantasyClient

STATUSES = ["A", "T"]
PAGE_SIZE = 25
REQUEST_DELAY = 0.6  # seconds between Yahoo API calls


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync per-date pitcher SV+H into pitcher_game_log.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", metavar="YYYY-MM-DD", help="Sync a specific date (default: yesterday).")
    group.add_argument("--since", metavar="YYYY-MM-DD", help="Backfill from this date through yesterday.")
    parser.add_argument("--force", action="store_true", help="Re-sync dates that already have data.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but do not write to database.")
    return parser.parse_args(argv)


def choose_league(requested_key: str | None, discovered_leagues: list[dict]) -> dict:
    if requested_key:
        for league in discovered_leagues:
            if league["league_key"] == requested_key:
                return league
        raise SystemExit(f"League key {requested_key!r} not found.")
    if len(discovered_leagues) == 1:
        return discovered_leagues[0]
    keys = ", ".join(lg["league_key"] for lg in discovered_leagues)
    raise SystemExit(f"Multiple leagues found; set yahoo_league_key in config. Found: {keys}")


def build_date_list(args: argparse.Namespace) -> list[date]:
    yesterday = date.today() - timedelta(days=1)
    if args.since:
        start = date.fromisoformat(args.since)
        result, d = [], start
        while d <= yesterday:
            result.append(d)
            d += timedelta(days=1)
        return result
    if args.date:
        return [date.fromisoformat(args.date)]
    return [yesterday]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_settings()
    client = YahooFantasyClient(settings)

    game = client.get_current_mlb_game()
    leagues = client.get_user_leagues_for_game(game["game_key"])
    league = choose_league(settings.yahoo_league_key, leagues)
    settings_payload = client.get_league_settings(league["league_key"])

    conn = connect(settings)
    try:
        league_id = upsert_league(conn, league, game, settings_payload)
        upsert_league_stat_categories(conn, league_id, settings_payload.get("categories", []))
        stat_id_map = load_stat_id_map(conn, league_id)
        conn.commit()
    except Exception:
        conn.close()
        raise

    sv_holds_stat_id = stat_id_map.get("SV+H")
    if sv_holds_stat_id is None:
        conn.close()
        raise SystemExit("SV+H stat not in league categories; run fantasy-pitcher-stats first.")

    target_dates = build_date_list(args)
    print(f"League: {league['league_key']}  SV+H stat_id={sv_holds_stat_id}  dates={len(target_dates)}")

    # Fetch and upsert the player list once — it is not date-specific.
    print("\nFetching current player list...")
    all_players: dict[str, dict] = {}
    for status in STATUSES:
        start = 0
        while True:
            time.sleep(REQUEST_DELAY)
            page = client.get_league_players_page(
                league["league_key"],
                status=status,
                position="P",
                start=start,
                count=PAGE_SIZE,
            )
            if not page:
                break
            for player in page:
                key = player["yahoo_player_key"]
                if key not in all_players:
                    all_players[key] = player
            print(f"  [{status} start={start}] {len(page)} players")
            if len(page) < PAGE_SIZE:
                break
            start += PAGE_SIZE

    print(f"Total pitchers: {len(all_players)}")

    player_id_map: dict[str, int] = {}
    if not args.dry_run:
        for player in all_players.values():
            player_id_map[player["yahoo_player_key"]] = upsert_player(conn, player)
        conn.commit()

    total_events = 0
    for game_date in target_dates:
        date_str = game_date.isoformat()

        if not args.force and game_log_date_exists(conn, date_str):
            print(f"{date_str}: already have data, skipping (--force to override)")
            continue

        if args.force:
            cur = conn.cursor()
            cur.execute("DELETE FROM pitcher_game_log WHERE game_date = ?", (date_str,))
            conn.commit()

        print(f"\n{date_str}")
        day_events = 0
        captured_at = utc_now().replace(tzinfo=None)

        for status in STATUSES:
            start = 0
            while True:
                time.sleep(REQUEST_DELAY)
                stats_map = client.get_league_players_stats_date_page(
                    league["league_key"],
                    game_date=date_str,
                    status=status,
                    position="P",
                    start=start,
                    count=PAGE_SIZE,
                )
                if not stats_map:
                    break

                page_events = 0
                for key, player_stats in stats_map.items():
                    raw = player_stats.get(sv_holds_stat_id)
                    try:
                        sv_holds = float(raw) if raw not in (None, "-") else 0.0
                    except (ValueError, TypeError):
                        sv_holds = 0.0

                    if sv_holds <= 0:
                        continue

                    player = all_players.get(key)
                    name = player["full_name"] if player else key
                    team = player.get("editorial_team_abbr") if player else "?"

                    if not args.dry_run and key in player_id_map:
                        insert_pitcher_game_log(
                            conn,
                            player_id=player_id_map[key],
                            game_date=date_str,
                            sv_holds=sv_holds,
                            captured_at_utc=captured_at,
                        )
                    day_events += 1
                    page_events += 1
                    print(f"  {name} ({team}) SV+H={sv_holds:.0f}")

                # stats_map has no reliable page-count signal; stop when a
                # full page was not returned (fewer keys than PAGE_SIZE).
                if len(stats_map) < PAGE_SIZE:
                    break
                start += PAGE_SIZE

        if not args.dry_run:
            conn.commit()
        print(f"  -> {day_events} SV+H event(s) stored")
        total_events += day_events

    conn.close()
    print(f"\nTotal: {total_events} event(s) across {len(target_dates)} date(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
