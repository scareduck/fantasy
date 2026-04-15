#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from fantasy.config import load_settings
from fantasy.db import (
    complete_sync_run,
    connect,
    create_sync_run,
    insert_availability_snapshot,
    insert_roster_snapshot,
    upsert_league,
    upsert_league_stat_categories,
    upsert_player,
)
from fantasy.utils import format_snapshot_timestamp, utc_now, write_csv
from fantasy.yahoo_client import YahooFantasyClient


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Yahoo Fantasy Baseball waiver pitchers into MariaDB.")
    parser.add_argument("--league-key", help="Yahoo league key. If omitted, auto-discover from your current MLB leagues.")
    parser.add_argument("--statuses", default="FA,W", help="Comma-separated availability statuses to pull. Default: FA,W")
    parser.add_argument("--position", default="P", help="League-context Yahoo position filter. Default: P")
    parser.add_argument("--page-size", type=int, default=25, help="Yahoo pagination count. Default: 25")
    parser.add_argument("--my-roster", action="store_true", help="Sync the current user's team roster into roster_snapshot.")
    parser.add_argument("--all-rosters", action="store_true", help="Sync every team's roster into roster_snapshot.")
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

    league_lines = "\n".join(f"  - {league['league_key']}: {league.get('name') or '(unnamed league)'}" for league in discovered_leagues)
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

    statuses = [item.strip() for item in args.statuses.split(",") if item.strip()]
    if not statuses:
        raise SystemExit("At least one Yahoo availability status is required.")

    run_ts = utc_now()
    timestamp_str = format_snapshot_timestamp(run_ts, settings.local_timezone)
    snapshot_path = settings.snapshot_dir / f"free_agents_{timestamp_str}.csv"

    game = client.get_current_mlb_game()
    leagues = client.get_user_leagues_for_game(game["game_key"])
    league = choose_league(args.league_key or settings.yahoo_league_key, leagues)
    settings_payload = client.get_league_settings(league["league_key"])

    print(f"Game:   {game['game_key']} ({game.get('season')})")
    print(f"League: {league['league_key']} - {league.get('name')}")
    print(f"Pull:   statuses={','.join(statuses)} position={args.position} page_size={args.page_size} my_roster={args.my_roster}")

    csv_rows: list[dict] = []
    db_rows: list[tuple[dict, str, int, int]] = []

    for status in statuses:
        start = 0
        while True:
            page = client.get_league_players_page(
                league["league_key"],
                status=status,
                position=args.position,
                start=start,
                count=args.page_size,
            )
            if not page:
                break

            for player in page:
                csv_rows.append(
                    {
                        "captured_at_utc": run_ts.isoformat(),
                        "league_key": league["league_key"],
                        "sync_status": status,
                        "yahoo_player_key": player["yahoo_player_key"],
                        "yahoo_player_id": player.get("yahoo_player_id"),
                        "full_name": player.get("full_name"),
                        "editorial_team_abbr": player.get("editorial_team_abbr"),
                        "editorial_team_full_name": player.get("editorial_team_full_name"),
                        "display_position": player.get("display_position"),
                        "position_type": player.get("position_type"),
                        "eligible_positions": "|".join(player.get("eligible_positions", [])),
                        "yahoo_status": player.get("yahoo_status"),
                        "yahoo_status_full": player.get("yahoo_status_full"),
                        "percent_owned": player.get("percent_owned"),
                    }
                )
                db_rows.append((player, status, start, args.page_size))

            print(f"Fetched {len(page):>3} players for status={status} start={start}")
            if len(page) < args.page_size:
                break
            start += args.page_size

    if not csv_rows:
        print("No players returned. Writing empty CSV snapshot anyway.")

    write_csv(
        snapshot_path,
        csv_rows,
        fieldnames=[
            "captured_at_utc",
            "league_key",
            "sync_status",
            "yahoo_player_key",
            "yahoo_player_id",
            "full_name",
            "editorial_team_abbr",
            "editorial_team_full_name",
            "display_position",
            "position_type",
            "eligible_positions",
            "yahoo_status",
            "yahoo_status_full",
            "percent_owned",
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
        sync_run_id = create_sync_run(
            conn,
            league_id=league_id,
            requested_position=args.position,
            requested_statuses=",".join(statuses),
            snapshot_file=str(snapshot_path),
            notes="Yahoo free-agent/waiver pitcher sync",
        )

        for player, status, source_page_start, source_page_count in db_rows:
            player_id = upsert_player(conn, player)
            insert_availability_snapshot(
                conn,
                sync_run_id=sync_run_id,
                league_id=league_id,
                player_id=player_id,
                captured_at_utc=run_ts.replace(tzinfo=None),
                availability_status=status,
                source_page_start=source_page_start,
                source_page_count=source_page_count,
                percent_owned=player.get("percent_owned"),
                raw_player_xml=player.get("raw_player_xml"),
            )

        complete_sync_run(conn, sync_run_id, len(db_rows))

        if args.my_roster or args.all_rosters:
            teams = client.get_league_teams(league["league_key"])
            if args.all_rosters:
                teams_to_sync = teams
            else:
                my_team = next((t for t in teams if t["is_owned_by_current_login"]), None)
                if my_team is None:
                    print("WARNING: Could not find a team owned by the current login; skipping roster sync.")
                    teams_to_sync = []
                else:
                    teams_to_sync = [my_team]

            total_roster_rows = 0
            for team in teams_to_sync:
                print(f"Roster: syncing team {team['team_key']} ({team['team_name']})")
                roster = client.get_team_roster(team["team_key"])
                for player in roster:
                    player_id = upsert_player(conn, player)
                    insert_roster_snapshot(
                        conn,
                        league_id=league_id,
                        team_key=team["team_key"],
                        team_name=team["team_name"],
                        player_id=player_id,
                        yahoo_player_key=player["yahoo_player_key"],
                        selected_position=player.get("selected_position"),
                        captured_at_utc=run_ts.replace(tzinfo=None),
                    )
                total_roster_rows += len(roster)
            if teams_to_sync:
                print(f"Inserted {total_roster_rows} roster snapshot rows across {len(teams_to_sync)} team(s)")

        conn.commit()
        print(f"Committed sync_run_id={sync_run_id} rows={len(db_rows)}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
