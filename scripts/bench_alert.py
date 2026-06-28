#!/usr/bin/env python3
"""Alert when active fantasy batters are not in the real-life MLB starting lineup."""
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
    """Return {home: {name, ...}, away: {name, ...}} if lineup is posted, else None."""
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
                return None  # Not posted yet
            return {
                "home": {normalize_name(p["fullName"]) for p in home_players},
                "away": {normalize_name(p["fullName"]) for p in away_players},
            }
    return None


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

        # Active (non-BN, non-IL) batters for our teams, excluding real-life IL/NA.
        placeholders = ",".join("?" * len(our_team_keys))
        cur.execute(
            f"""
            SELECT cr.team_key,
                   p.player_id,
                   p.full_name,
                   p.ascii_first_name,
                   p.ascii_last_name,
                   p.editorial_team_abbr
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
        batters = [
            {
                "team_key":  row[0],
                "player_id": row[1],
                "full_name": row[2],
                "norm_name": normalize_name(
                    f"{row[3] or ''} {row[4] or ''}".strip() or row[2]
                ),
                "mlb_team":  row[5] or "",
            }
            for row in cur.fetchall()
        ]

        if not batters:
            print("No active batters found for our teams.")
            return 0

        mlb_teams = {b["mlb_team"] for b in batters}

        # Today's games that haven't started yet (30-min grace window).
        cur.execute("""
            SELECT game_pk, home_team_abbr, away_team_abbr
            FROM mlb_schedule
            WHERE game_date = CURDATE()
              AND game_datetime_utc > UTC_TIMESTAMP() - INTERVAL 30 MINUTE
        """)
        games = [
            {"game_pk": row[0], "home": row[1], "away": row[2]}
            for row in cur.fetchall()
            if row[1] in mlb_teams or row[2] in mlb_teams
        ]

        if not games:
            print("No upcoming games today for our active batters.")
            return 0

        print(
            f"Checking {len(games)} game(s) for {len(batters)} active batter(s)..."
        )

        alerts_by_email: dict[str, list[str]] = {}
        new_alerts: list[tuple[str, int, int]] = []  # (today, player_id, game_pk)

        for game in games:
            game_pk  = game["game_pk"]
            home_abbr = game["home"]
            away_abbr = game["away"]

            lineup = fetch_lineup(game_pk)
            if lineup is None:
                print(f"  {away_abbr} @ {home_abbr}: lineup not yet posted.")
                continue

            print(f"  {away_abbr} @ {home_abbr}: lineup posted.")

            for batter in batters:
                mlb_team = batter["mlb_team"]
                if mlb_team not in (home_abbr, away_abbr):
                    continue

                side = "home" if mlb_team == home_abbr else "away"
                if batter["norm_name"] in lineup[side]:
                    continue  # In the lineup, all good.

                player_id = batter["player_id"]

                # Check dedup table.
                cur.execute(
                    "SELECT 1 FROM bench_alert WHERE alert_date = ? AND player_id = ?",
                    (today, player_id),
                )
                if cur.fetchone():
                    continue  # Already alerted today.

                matchup = f"{away_abbr} @ {home_abbr}"
                line = f"{batter['full_name']} ({mlb_team}) — not in lineup ({matchup})"
                print(f"  ALERT: {line}")

                email = team_to_email[batter["team_key"]]
                alerts_by_email.setdefault(email, []).append(line)
                new_alerts.append((today, player_id, game_pk))

        # Send emails.
        for email, lines in alerts_by_email.items():
            subject = (
                f"Bench Alert: {len(lines)} player{'s' if len(lines) != 1 else ''} "
                f"sitting today"
            )
            body = (
                "The following active roster players are NOT in today's MLB lineup:\n\n"
                + "\n".join(f"  • {line}" for line in lines)
                + "\n\nCheck your lineup — you may want to make a substitution."
            )
            send_alert(subject, body, to=email)
            print(f"  Sent alert to {email}")

        # Record alerts after sending (so a send failure doesn't silently suppress future alerts).
        for alert_date, player_id, game_pk in new_alerts:
            cur.execute(
                "INSERT IGNORE INTO bench_alert (alert_date, player_id, game_pk) VALUES (?, ?, ?)",
                (alert_date, player_id, game_pk),
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
