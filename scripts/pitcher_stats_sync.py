#!/usr/bin/env python3
"""Sync Yahoo Fantasy pitcher season stats into MariaDB.

Fetches both available pitchers (status=A: FA + waivers) and rostered
pitchers (status=T) so that probable starters on other teams' rosters
are covered for the lineup matchup queries.
"""
from __future__ import annotations

import argparse

from fantasy.config import load_settings
from fantasy.db import (
    complete_sync_run,
    connect,
    create_sync_run,
    load_stat_id_map,
    upsert_league,
    upsert_league_stat_categories,
    upsert_pitcher_season_stats,
    upsert_player,
)
from fantasy.utils import format_snapshot_timestamp, utc_now, write_csv
from fantasy.yahoo_client import YahooFantasyClient

STATUSES = ["A", "T"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Yahoo Fantasy pitcher season stats for all pitchers.")
    parser.add_argument("--league-key", help="Yahoo league key. If omitted, auto-discover from your current MLB leagues.")
    parser.add_argument("--page-size", type=int, default=25, help="Yahoo pagination count. Default: 25")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and write CSV but do not write to MariaDB.")
    return parser.parse_args(argv)


def choose_league(requested_key: str | None, discovered_leagues: list[dict]) -> dict:
    if requested_key:
        for league in discovered_leagues:
            if league["league_key"] == requested_key:
                return league
        raise SystemExit(f"Requested league key {requested_key!r} was not found in the authenticated user's MLB leagues.")

    if len(discovered_leagues) == 1:
        return discovered_leagues[0]

    league_lines = "\n".join(
        f"  - {league['league_key']}: {league.get('name') or '(unnamed league)'}"
        for league in discovered_leagues
    )
    raise SystemExit(
        "Multiple MLB leagues were found for this Yahoo account. Re-run with --league-key.\n"
        f"Available leagues:\n{league_lines}"
    )


def main() -> int:
    args = parse_args()
    return run(args)


def run(args: argparse.Namespace) -> int:
    settings = load_settings()
    client = YahooFantasyClient(settings)

    run_ts = utc_now()
    timestamp_str = format_snapshot_timestamp(run_ts, settings.local_timezone)
    snapshot_path = settings.snapshot_dir / f"pitcher_stats_{timestamp_str}.csv"

    game = client.get_current_mlb_game()
    leagues = client.get_user_leagues_for_game(game["game_key"])
    league = choose_league(getattr(args, "league_key", None) or settings.yahoo_league_key, leagues)
    settings_payload = client.get_league_settings(league["league_key"])

    print(f"Game:   {game['game_key']} ({game.get('season')})")
    print(f"League: {league['league_key']} - {league.get('name')}")
    print(f"Pull:   statuses={','.join(STATUSES)} position=P page_size={args.page_size}")

    # (player_dict, stats_dict) — keyed by yahoo_player_key to deduplicate
    seen: set[str] = set()
    all_rows: list[tuple[dict, dict[int, str | None]]] = []
    csv_rows: list[dict] = []

    for status in STATUSES:
        start = 0
        while True:
            page = client.get_league_players_page(
                league["league_key"],
                status=status,
                position="P",
                start=start,
                count=args.page_size,
            )
            if not page:
                break

            stats_map = client.get_league_players_stats_page(
                league["league_key"],
                status=status,
                position="P",
                start=start,
                count=args.page_size,
            )

            new_players = [p for p in page if p["yahoo_player_key"] not in seen]
            for player in new_players:
                player_key = player["yahoo_player_key"]
                seen.add(player_key)
                player_stats = stats_map.get(player_key, {})
                csv_rows.append(
                    {
                        "captured_at_utc": run_ts.isoformat(),
                        "status": status,
                        "yahoo_player_key": player_key,
                        "full_name": player.get("full_name"),
                        "editorial_team_abbr": player.get("editorial_team_abbr"),
                        "display_position": player.get("display_position"),
                        "stat_ids": str(sorted(player_stats.keys())),
                    }
                )
                all_rows.append((player, player_stats))

            print(f"Fetched {len(page):>3} pitchers (status={status} start={start}, {len(new_players)} new)")
            if len(page) < args.page_size:
                break
            start += args.page_size

    if not csv_rows:
        print("No players returned. Writing empty CSV snapshot anyway.")

    write_csv(
        snapshot_path,
        csv_rows,
        fieldnames=[
            "captured_at_utc", "status", "yahoo_player_key", "full_name",
            "editorial_team_abbr", "display_position", "stat_ids",
        ],
    )
    print(f"Wrote CSV snapshot: {snapshot_path}")

    if args.dry_run:
        print("Dry run enabled; skipping database writes.")
        return 0

    conn = connect(settings)
    try:
        league_id = upsert_league(conn, league, game, settings_payload)
        upsert_league_stat_categories(conn, league_id, settings_payload.get("categories", []))
        stat_id_map = load_stat_id_map(conn, league_id)

        sync_run_id = create_sync_run(
            conn,
            league_id=league_id,
            requested_position="P",
            requested_statuses=",".join(STATUSES),
            snapshot_file=str(snapshot_path),
            notes="Yahoo all-pitcher season stats sync (available + rostered)",
        )

        for player, player_stats in all_rows:
            player_id = upsert_player(conn, player)
            upsert_pitcher_season_stats(
                conn,
                sync_run_id=sync_run_id,
                player_id=player_id,
                captured_at_utc=run_ts.replace(tzinfo=None),
                stat_map=stat_id_map,
                stats=player_stats,
            )

        complete_sync_run(conn, sync_run_id, len(all_rows))
        conn.commit()
        print(f"Committed sync_run_id={sync_run_id} rows={len(all_rows)}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
