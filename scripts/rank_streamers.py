#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Iterable

from fantasy_baseball.config import load_settings
from fantasy_baseball.db import connect


@dataclass
class RankedPlayer:
    bucket: str
    score: float
    reasons: list[str]
    row: dict



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank available Yahoo pitchers by category-fit buckets.")
    parser.add_argument(
        "--bucket",
        choices=["all", "safe_ratio", "kw_chase", "svh_dart"],
        default="all",
        help="Ranking bucket. Default: all",
    )
    parser.add_argument("--limit", type=int, default=15, help="Rows per bucket. Default: 15")
    parser.add_argument("--sync-run-id", type=int, help="Use a specific sync_run_id instead of the latest one")
    parser.add_argument("--league-key", help="Limit latest sync lookup to a specific league key")
    return parser.parse_args()



def resolve_sync_run_id(conn, explicit_sync_run_id: int | None, league_key: str | None) -> int:
    if explicit_sync_run_id:
        return explicit_sync_run_id

    cur = conn.cursor(dictionary=True)
    if league_key:
        cur.execute(
            """
            SELECT sr.sync_run_id
            FROM sync_run sr
            JOIN league l ON l.league_id = sr.league_id
            WHERE l.yahoo_league_key = ?
            ORDER BY sr.started_at_utc DESC
            LIMIT 1
            """,
            (league_key,),
        )
    else:
        cur.execute(
            "SELECT sync_run_id FROM sync_run ORDER BY started_at_utc DESC LIMIT 1"
        )
    row = cur.fetchone()
    if row is None:
        raise SystemExit("No sync_run rows found. Run scripts/yahoo_sync.py first.")
    return int(row["sync_run_id"])



def fetch_snapshot_rows(conn, sync_run_id: int) -> list[dict]:
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
            p.player_id,
            p.yahoo_player_key,
            p.full_name,
            p.editorial_team_abbr,
            p.display_position,
            p.position_type,
            pas.availability_status,
            pas.percent_owned,
            ps.start_date,
            ps.opponent_team_abbr AS probable_opp,
            ps.is_home,
            ps.park AS probable_park,
            ps.role_code,
            pr.innings,
            pr.wins,
            pr.strikeouts,
            pr.era,
            pr.whip,
            pr.sv_holds,
            pr.espn_fpts,
            pr.opponent_team_abbr AS projection_opp,
            pr.park AS projection_park,
            GROUP_CONCAT(DISTINCT CASE WHEN sn.is_active = 1 THEN sn.tag END ORDER BY sn.tag SEPARATOR '|') AS active_tags
        FROM player_availability_snapshot pas
        JOIN player p
            ON p.player_id = pas.player_id
        LEFT JOIN probable_start ps
            ON ps.probable_start_id = (
                SELECT MAX(ps2.probable_start_id)
                FROM probable_start ps2
                WHERE ps2.player_id = p.player_id
            )
        LEFT JOIN projection pr
            ON pr.projection_id = (
                SELECT MAX(pr2.projection_id)
                FROM projection pr2
                WHERE pr2.player_id = p.player_id
            )
        LEFT JOIN stream_note sn
            ON sn.player_id = p.player_id
        WHERE pas.sync_run_id = ?
        GROUP BY
            p.player_id,
            p.yahoo_player_key,
            p.full_name,
            p.editorial_team_abbr,
            p.display_position,
            p.position_type,
            pas.availability_status,
            pas.percent_owned,
            ps.start_date,
            ps.opponent_team_abbr,
            ps.is_home,
            ps.park,
            ps.role_code,
            pr.innings,
            pr.wins,
            pr.strikeouts,
            pr.era,
            pr.whip,
            pr.sv_holds,
            pr.espn_fpts,
            pr.opponent_team_abbr,
            pr.park
        ORDER BY p.full_name
        """,
        (sync_run_id,),
    )
    return list(cur.fetchall())



def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))



def parse_tags(tag_string: str | None) -> set[str]:
    if not tag_string:
        return set()
    return {tag.strip() for tag in tag_string.split("|") if tag.strip()}



def park_penalty(park: str | None) -> tuple[float, str | None]:
    if not park:
        return 0.0, None
    text = park.lower()
    if "coors" in text:
        return -18.0, "Coors penalty"
    if "great american" in text:
        return -8.0, "homer park penalty"
    if "oakland" in text or "safeco" in text or "t-mobile" in text or "petco" in text:
        return 6.0, "pitcher park bonus"
    return 0.0, None



def probable_start_bonus(row: dict) -> tuple[float, str | None]:
    role = (row.get("role_code") or "").lower()
    display_position = (row.get("display_position") or "").upper()
    if row.get("start_date"):
        if role in {"sp", "starter", "probable"} or "SP" in display_position:
            return 40.0, "confirmed/likely start"
        return 28.0, "scheduled appearance"
    return 0.0, None



def tie_breaker_rostered(percent_owned) -> float:
    if percent_owned is None:
        return 0.0
    return clamp(float(percent_owned) * 0.04, 0.0, 4.0)



def score_safe_ratio(row: dict) -> RankedPlayer:
    score = 0.0
    reasons: list[str] = []
    tags = parse_tags(row.get("active_tags"))

    bonus, reason = probable_start_bonus(row)
    score += bonus
    if reason:
        reasons.append(reason)

    innings = row.get("innings")
    if innings is not None:
        v = clamp(float(innings) * 2.5, 0.0, 15.0)
        score += v
        reasons.append(f"IP {innings}")

    era = row.get("era")
    if era is not None:
        v = clamp((4.20 - float(era)) * 8.0, -18.0, 18.0)
        score += v
        reasons.append(f"ERA {era}")

    whip = row.get("whip")
    if whip is not None:
        v = clamp((1.32 - float(whip)) * 25.0, -18.0, 18.0)
        score += v
        reasons.append(f"WHIP {whip}")

    if "ratio_safe" in tags:
        score += 14.0
        reasons.append("ratio_safe tag")

    park = row.get("probable_park") or row.get("projection_park")
    v, reason = park_penalty(park)
    score += v
    if reason:
        reasons.append(reason)

    if row.get("display_position") and "RP" in str(row.get("display_position")) and not row.get("start_date"):
        score -= 12.0
        reasons.append("reliever without start")

    score += tie_breaker_rostered(row.get("percent_owned"))
    return RankedPlayer("safe_ratio", round(score, 2), reasons, row)



def score_kw_chase(row: dict) -> RankedPlayer:
    score = 0.0
    reasons: list[str] = []
    tags = parse_tags(row.get("active_tags"))

    bonus, reason = probable_start_bonus(row)
    score += bonus
    if reason:
        reasons.append(reason)

    strikeouts = row.get("strikeouts")
    if strikeouts is not None:
        v = clamp(float(strikeouts) * 3.2, 0.0, 35.0)
        score += v
        reasons.append(f"K {strikeouts}")

    wins = row.get("wins")
    if wins is not None:
        v = clamp(float(wins) * 12.0, 0.0, 18.0)
        score += v
        reasons.append(f"W {wins}")

    innings = row.get("innings")
    if innings is not None:
        v = clamp(float(innings) * 1.8, 0.0, 12.0)
        score += v
        reasons.append(f"IP {innings}")

    era = row.get("era")
    if era is not None and float(era) > 4.30:
        penalty = clamp((float(era) - 4.30) * 4.5, 0.0, 14.0)
        score -= penalty
        reasons.append("ERA risk")

    whip = row.get("whip")
    if whip is not None and float(whip) > 1.34:
        penalty = clamp((float(whip) - 1.34) * 16.0, 0.0, 14.0)
        score -= penalty
        reasons.append("WHIP risk")

    if "k_w_chase" in tags:
        score += 14.0
        reasons.append("k_w_chase tag")

    score += tie_breaker_rostered(row.get("percent_owned"))
    return RankedPlayer("kw_chase", round(score, 2), reasons, row)



def score_svh_dart(row: dict) -> RankedPlayer:
    score = 0.0
    reasons: list[str] = []
    tags = parse_tags(row.get("active_tags"))
    display_position = (row.get("display_position") or "").upper()

    if "RP" in display_position:
        score += 24.0
        reasons.append("relief role")
    elif row.get("start_date"):
        score -= 12.0
        reasons.append("starter penalty")

    sv_holds = row.get("sv_holds")
    if sv_holds is not None:
        v = clamp(float(sv_holds) * 10.0, 0.0, 30.0)
        score += v
        reasons.append(f"SV+H {sv_holds}")

    era = row.get("era")
    if era is not None:
        v = clamp((3.80 - float(era)) * 7.0, -14.0, 14.0)
        score += v
        reasons.append(f"ERA {era}")

    whip = row.get("whip")
    if whip is not None:
        v = clamp((1.25 - float(whip)) * 18.0, -12.0, 12.0)
        score += v
        reasons.append(f"WHIP {whip}")

    if "svh_dart" in tags:
        score += 16.0
        reasons.append("svh_dart tag")
    if "leverage" in tags:
        score += 8.0
        reasons.append("leverage tag")

    score += tie_breaker_rostered(row.get("percent_owned"))
    return RankedPlayer("svh_dart", round(score, 2), reasons, row)



def format_line(rank: int, item: RankedPlayer) -> str:
    row = item.row
    opp = row.get("probable_opp") or row.get("projection_opp") or "-"
    park = row.get("probable_park") or row.get("projection_park") or "-"
    tags = row.get("active_tags") or "-"
    summary = ", ".join(item.reasons[:4])
    return (
        f"{rank:>2}. {row['full_name']} ({row.get('editorial_team_abbr') or '-'}) "
        f"[{row.get('availability_status')}] score={item.score:.2f} "
        f"opp={opp} park={park} tags={tags} | {summary}"
    )



def main() -> int:
    args = parse_args()
    settings = load_settings()
    conn = connect(settings)
    try:
        sync_run_id = resolve_sync_run_id(conn, args.sync_run_id, args.league_key)
        rows = fetch_snapshot_rows(conn, sync_run_id)
    finally:
        conn.close()

    if not rows:
        raise SystemExit(f"No player snapshot rows found for sync_run_id={sync_run_id}")

    buckets = {
        "safe_ratio": [score_safe_ratio(row) for row in rows],
        "kw_chase": [score_kw_chase(row) for row in rows],
        "svh_dart": [score_svh_dart(row) for row in rows],
    }
    for key in buckets:
        buckets[key].sort(key=lambda item: item.score, reverse=True)

    selected = [args.bucket] if args.bucket != "all" else ["safe_ratio", "kw_chase", "svh_dart"]
    print(f"Using sync_run_id={sync_run_id}\n")

    for bucket in selected:
        print(bucket)
        print("-" * len(bucket))
        for idx, item in enumerate(buckets[bucket][: args.limit], start=1):
            print(format_line(idx, item))
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
