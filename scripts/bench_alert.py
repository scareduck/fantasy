#!/usr/bin/env python3
"""Bench alerts:
  Case 1 — active fantasy batter not in IRL starting lineup.
  Case 2 — player marked "regular" is on BN but IS in IRL lineup.
  Case 3 — probable SP is in an active fantasy slot but no longer listed as probable.
"""
from __future__ import annotations

import json
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

from fantasy.config import load_settings
from fantasy.db import connect
from fantasy.notify import send_alert

MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
REPORTS_PATH = Path(__file__).parent.parent / "reports.json"


def normalize_name(name: str) -> str:
    return (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode()
        .lower()
        .strip()
    )


def fetch_lineup(game_pk: int) -> dict[str, set[str]] | None:
    """Return {home: {name,...}, away: {name,...}} if batting lineup is posted, else None."""
    url = f"{MLB_SCHEDULE_URL}?sportId=1&gamePk={game_pk}&hydrate=lineups"
    try:
        with urlopen(url, timeout=15) as resp:
            data = json.load(resp)
    except URLError as exc:
        print(f"  WARNING: could not fetch lineup for game {game_pk}: {exc}")
        return None

    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            lineups = game.get("lineups") or {}
            home_players = lineups.get("homePlayers") or []
            away_players = lineups.get("awayPlayers") or []
            if not home_players and not away_players:
                return None
            return {
                "home": {normalize_name(p["fullName"]) for p in home_players},
                "away": {normalize_name(p["fullName"]) for p in away_players},
            }
    return None


def fetch_live_probable_pitchers(date_str: str) -> dict[int, dict[str, str | None]]:
    """Return {game_pk: {home: norm_name|None, away: norm_name|None}} from live API."""
    url = f"{MLB_SCHEDULE_URL}?sportId=1&date={date_str}&hydrate=probablePitcher,team"
    try:
        with urlopen(url, timeout=15) as resp:
            data = json.load(resp)
    except URLError as exc:
        print(f"  WARNING: could not fetch live schedule: {exc}")
        return {}
    result: dict[int, dict[str, str | None]] = {}
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            if game.get("gameType") != "R":
                continue
            game_pk = game["gamePk"]
            teams = game.get("teams", {})
            home_p = (teams.get("home") or {}).get("probablePitcher") or {}
            away_p = (teams.get("away") or {}).get("probablePitcher") or {}
            result[game_pk] = {
                "home": normalize_name(home_p["fullName"]) if home_p.get("fullName") else None,
                "away": normalize_name(away_p["fullName"]) if away_p.get("fullName") else None,
            }
    return result


def already_alerted(cur, today: str, player_id: int, alert_type: str) -> bool:
    cur.execute(
        "SELECT 1 FROM bench_alert WHERE alert_date = ? AND player_id = ? AND alert_type = ?",
        (today, player_id, alert_type),
    )
    return cur.fetchone() is not None


def _batter_row(row: tuple, alert_type: str) -> dict:
    return {
        "team_key":   row[0],
        "player_id":  row[1],
        "full_name":  row[2],
        "norm_name":  normalize_name(
            f"{row[3] or ''} {row[4] or ''}".strip() or row[2]
        ),
        "mlb_team":     row[5] or "",
        "yahoo_status": row[6],
        "alert_type":   alert_type,
    }


def main(argv: list[str] | None = None) -> int:
    with open(REPORTS_PATH) as f:
        reports = json.load(f)
    team_to_email: dict[str, str] = {r["team_key"]: r["email"] for r in reports}
    our_team_keys = list(team_to_email.keys())

    settings = load_settings()
    conn = connect(settings)
    today = datetime.now(timezone.utc).date().isoformat()

    try:
        cur = conn.cursor()
        placeholders = ",".join("?" * len(our_team_keys))

        # Case 1: active (non-BN/IL) batters, excluding IRL IL/NA.
        cur.execute(
            f"""
            SELECT cr.team_key, p.player_id, p.full_name,
                   p.ascii_first_name, p.ascii_last_name, p.editorial_team_abbr,
                   p.yahoo_status
            FROM current_roster cr
            JOIN player p ON p.player_id = cr.player_id
            WHERE cr.team_key IN ({placeholders})
              AND cr.selected_position NOT IN ('BN', 'IL')
              AND p.position_type = 'B'
              AND (p.yahoo_status IS NULL
                   OR (p.yahoo_status NOT LIKE 'IL%' AND p.yahoo_status != 'NA'))
            """,
            our_team_keys,
        )
        active_batters = [_batter_row(r, "benched") for r in cur.fetchall()]

        # Case 2: marked-regular batters currently on BN, excluding IRL IL/NA.
        cur.execute(
            f"""
            SELECT cr.team_key, p.player_id, p.full_name,
                   p.ascii_first_name, p.ascii_last_name, p.editorial_team_abbr,
                   p.yahoo_status
            FROM current_roster cr
            JOIN player p ON p.player_id = cr.player_id
            JOIN player_regular pr ON pr.player_id = cr.player_id
            WHERE cr.team_key IN ({placeholders})
              AND cr.selected_position = 'BN'
              AND p.position_type = 'B'
              AND (p.yahoo_status IS NULL
                   OR (p.yahoo_status NOT LIKE 'IL%' AND p.yahoo_status != 'NA'))
            """,
            our_team_keys,
        )
        bench_regulars = [_batter_row(r, "regular_benched") for r in cur.fetchall()]

        # Case 3: active SP-slot pitchers who were the probable starter for a game today.
        cur.execute(
            f"""
            SELECT cr.team_key, p.player_id, p.full_name,
                   p.ascii_first_name, p.ascii_last_name, p.editorial_team_abbr,
                   s.game_pk, s.home_team_abbr, s.away_team_abbr,
                   CASE WHEN s.home_pitcher_player_id = p.player_id THEN 'home' ELSE 'away' END AS side
            FROM current_roster cr
            JOIN player p ON p.player_id = cr.player_id
            JOIN mlb_schedule s
              ON (s.home_pitcher_player_id = p.player_id OR s.away_pitcher_player_id = p.player_id)
             AND s.game_date = CURDATE()
             AND s.game_datetime_utc > UTC_TIMESTAMP() - INTERVAL 30 MINUTE
            WHERE cr.team_key IN ({placeholders})
              AND cr.selected_position NOT IN ('BN', 'IL')
              AND p.position_type = 'P'
              AND (p.yahoo_status IS NULL
                   OR (p.yahoo_status NOT LIKE 'IL%' AND p.yahoo_status != 'NA'))
            """,
            our_team_keys,
        )
        probable_pitchers = [
            {
                "team_key":   row[0],
                "player_id":  row[1],
                "full_name":  row[2],
                "norm_name":  normalize_name(
                    f"{row[3] or ''} {row[4] or ''}".strip() or row[2]
                ),
                "mlb_team":   row[5] or "",
                "game_pk":    row[6],
                "home":       row[7],
                "away":       row[8],
                "side":       row[9],
                "alert_type": "scratched",
            }
            for row in cur.fetchall()
        ]

        # Collect MLB teams needed for lineup fetches (Cases 1 & 2).
        batter_teams = {b["mlb_team"] for b in active_batters + bench_regulars}

        # Today's relevant games (within 30-min grace window).
        cur.execute("""
            SELECT game_pk, home_team_abbr, away_team_abbr
            FROM mlb_schedule
            WHERE game_date = CURDATE()
              AND game_datetime_utc > UTC_TIMESTAMP() - INTERVAL 30 MINUTE
        """)
        games = [
            {"game_pk": row[0], "home": row[1], "away": row[2]}
            for row in cur.fetchall()
            if row[1] in batter_teams or row[2] in batter_teams
        ]

        # email -> [(alert_type, line)]
        alerts_by_email: dict[str, list[tuple[str, str]]] = {}
        # (today, player_id, game_pk, alert_type)
        new_alerts: list[tuple[str, int, int, str]] = []

        # ── Cases 1 & 2: check batting lineups per game ───────────────────────
        for game in games:
            game_pk   = game["game_pk"]
            home_abbr = game["home"]
            away_abbr = game["away"]

            lineup = fetch_lineup(game_pk)
            if lineup is None:
                print(f"  {away_abbr} @ {home_abbr}: lineup not yet posted.")
                continue

            print(f"  {away_abbr} @ {home_abbr}: lineup posted.")

            for batter in active_batters:
                if batter["mlb_team"] not in (home_abbr, away_abbr):
                    continue
                side = "home" if batter["mlb_team"] == home_abbr else "away"
                if not lineup[side]:
                    continue  # This team's half not posted yet — skip.
                if batter["norm_name"] in lineup[side]:
                    continue
                player_id = batter["player_id"]
                if already_alerted(cur, today, player_id, "benched"):
                    continue
                status_tag = f" [{batter['yahoo_status']}]" if batter["yahoo_status"] else ""
                line = (
                    f"{batter['full_name']} ({batter['mlb_team']}) "
                    f"— not in lineup ({away_abbr} @ {home_abbr}){status_tag}"
                )
                print(f"  BENCH: {line}")
                alerts_by_email.setdefault(team_to_email[batter["team_key"]], []).append(
                    ("benched", line)
                )
                new_alerts.append((today, player_id, game_pk, "benched"))

            for batter in bench_regulars:
                if batter["mlb_team"] not in (home_abbr, away_abbr):
                    continue
                side = "home" if batter["mlb_team"] == home_abbr else "away"
                if not lineup[side]:
                    continue  # This team's half not posted yet — skip.
                if batter["norm_name"] not in lineup[side]:
                    continue  # Not in lineup, no alert needed.
                player_id = batter["player_id"]
                if already_alerted(cur, today, player_id, "regular_benched"):
                    continue
                line = (
                    f"{batter['full_name']} ({batter['mlb_team']}) "
                    f"— in lineup but sitting on your bench ({away_abbr} @ {home_abbr})"
                )
                print(f"  REGULAR: {line}")
                alerts_by_email.setdefault(team_to_email[batter["team_key"]], []).append(
                    ("regular_benched", line)
                )
                new_alerts.append((today, player_id, game_pk, "regular_benched"))

        # ── Case 3: check for scratched probable starters ─────────────────────
        if probable_pitchers:
            live_probables = fetch_live_probable_pitchers(today)
            for pitcher in probable_pitchers:
                game_pk = pitcher["game_pk"]
                side    = pitcher["side"]
                current = (live_probables.get(game_pk) or {}).get(side)
                if current and current == pitcher["norm_name"]:
                    continue  # Still the listed probable — all good.
                player_id = pitcher["player_id"]
                if already_alerted(cur, today, player_id, "scratched"):
                    continue
                matchup = f"{pitcher['away']} @ {pitcher['home']}"
                line = (
                    f"{pitcher['full_name']} ({pitcher['mlb_team']}) "
                    f"— expected to start {matchup} but no longer listed as probable"
                )
                print(f"  SCRATCH: {line}")
                alerts_by_email.setdefault(team_to_email[pitcher["team_key"]], []).append(
                    ("scratched", line)
                )
                new_alerts.append((today, player_id, game_pk, "scratched"))

        # ── Send one combined email per owner ─────────────────────────────────
        for email, tagged_lines in alerts_by_email.items():
            benched   = [l for t, l in tagged_lines if t == "benched"]
            regulars  = [l for t, l in tagged_lines if t == "regular_benched"]
            scratched = [l for t, l in tagged_lines if t == "scratched"]

            sections: list[str] = []
            if benched:
                sections.append(
                    "ACTIVE PLAYERS NOT IN LINEUP\n"
                    + "\n".join(f"  • {l}" for l in benched)
                )
            if regulars:
                sections.append(
                    "REGULARS SITTING ON YOUR BENCH\n"
                    + "\n".join(f"  • {l}" for l in regulars)
                )
            if scratched:
                sections.append(
                    "STARTING PITCHERS SCRATCHED\n"
                    + "\n".join(f"  • {l}" for l in scratched)
                )

            n = len(tagged_lines)
            subject = f"Bench Alert: {n} issue{'s' if n != 1 else ''} today"
            body = "\n\n".join(sections) + "\n\nCheck your lineup."
            send_alert(subject, body, to=email)
            print(f"  Sent alert to {email}")

        # Record after sending so a send failure does not suppress future alerts.
        for alert_date, player_id, game_pk, alert_type in new_alerts:
            cur.execute(
                "INSERT IGNORE INTO bench_alert "
                "(alert_date, player_id, game_pk, alert_type) VALUES (?, ?, ?, ?)",
                (alert_date, player_id, game_pk, alert_type),
            )

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
