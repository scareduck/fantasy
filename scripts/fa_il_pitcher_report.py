#!/usr/bin/env python3
"""Identify FA/waiver IL pitchers and use Claude to score injury severity,
estimate return dates, and flag high-quality targets.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import anthropic
import mariadb

from fantasy.config import load_anthropic_api_key, load_settings
from fantasy.db import connect, upsert_fa_il_analysis
from fantasy.yahoo_xml import NS, find_text

CLAUDE_MODEL = "claude-sonnet-4-6"

SYSTEM_PROMPT = """You are a baseball injury analyst for a fantasy baseball application.
Given a pitcher's injury status, injury note, and season stats, you will assess:

Severity scale (injury_severity):
  1 = Minor strain, blister, general soreness — likely back in 1–2 weeks
  2 = Hamstring, oblique, or groin injury — typically 3–6 weeks
  3 = UCL sprain, elbow inflammation, shoulder tendinitis — 6–12 weeks or more
  4 = Labrum tear, significant shoulder surgery — often 4–9 months
  5 = Tommy John surgery, career-threatening injury — typically 12–18 months

High-quality standard (is_high_quality):
  True if the pitcher has demonstrated above-average MLB performance:
  career or recent-season ERA under 3.75, K/9 above 9.0, or equivalent
  credentials (e.g., former All-Star, ace-level track record).

Return date estimation:
  Use specific dates from news when available.
  For vague language ("late June" → June 26, "mid-July" → July 15,
  "early August" → August 5). Always set return_date_is_estimate = true
  when inferring from vague language or when no date is mentioned.
  Set return_date = null if there is genuinely no information.

Today's date: """ + datetime.now(timezone.utc).strftime("%Y-%m-%d") + """

Respond only by calling the record_pitcher_analysis tool."""

ANALYSIS_TOOL: dict = {
    "name": "record_pitcher_analysis",
    "description": "Record the structured injury analysis for a pitcher.",
    "input_schema": {
        "type": "object",
        "properties": {
            "injury_description": {
                "type": "string",
                "description": "Brief description of the injury (1-2 sentences max).",
            },
            "injury_severity": {
                "type": "integer",
                "description": "Severity score 1–5 per the scale in the system prompt.",
                "minimum": 1,
                "maximum": 5,
            },
            "return_date": {
                "type": ["string", "null"],
                "description": "Projected return-to-active-roster date as YYYY-MM-DD, or null.",
            },
            "return_date_is_estimate": {
                "type": "boolean",
                "description": "True if the date is estimated or inferred from vague language.",
            },
            "is_high_quality": {
                "type": "boolean",
                "description": "True if the pitcher meets the high-quality standard.",
            },
            "quality_notes": {
                "type": "string",
                "description": "One sentence explaining the quality assessment.",
            },
            "return_notes": {
                "type": "string",
                "description": "One sentence explaining how the return date was determined.",
            },
        },
        "required": [
            "injury_description", "injury_severity", "return_date",
            "return_date_is_estimate", "is_high_quality",
            "quality_notes", "return_notes",
        ],
    },
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score FA IL pitchers using Claude AI."
    )
    parser.add_argument("--force", action="store_true",
                        help="Re-analyze all pitchers, ignoring recency.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print analysis without writing to DB.")
    parser.add_argument("--player", metavar="NAME",
                        help="Analyze a single player by name (partial match).")
    return parser.parse_args(argv)


def extract_injury_note(raw_player_xml: str | None) -> str | None:
    if not raw_player_xml:
        return None
    try:
        root = ET.fromstring(raw_player_xml)
        return find_text(root, "y:injury_note")
    except Exception:
        return None


def get_fa_il_pitchers(conn: mariadb.Connection, *, player_name: str | None,
                        force: bool) -> list[dict]:
    cur = conn.cursor(dictionary=True)
    recency_clause = "" if force else (
        "AND (a.analyzed_at_utc IS NULL "
        "     OR a.analyzed_at_utc < UTC_TIMESTAMP() - INTERVAL 23 HOUR)"
    )
    name_clause = ""
    params: list = []
    if player_name:
        name_clause = "AND p.full_name LIKE ?"
        params.append(f"%{player_name}%")

    cur.execute(
        f"""
        SELECT
            p.player_id,
            p.full_name,
            p.yahoo_player_key,
            p.editorial_team_abbr  AS team,
            p.display_position     AS pos,
            p.yahoo_status         AS il_type,
            p.yahoo_status_full,
            p.raw_player_xml,
            ca.availability_status AS avail,
            cps.era,
            cps.whip,
            cps.ip,
            cps.k,
            a.analyzed_at_utc
        FROM current_availability ca
        JOIN player p ON p.player_id = ca.player_id
        LEFT JOIN current_pitcher_stats cps ON cps.player_id = ca.player_id
        LEFT JOIN fa_il_pitcher_analysis a   ON a.player_id  = ca.player_id
        WHERE p.yahoo_status LIKE 'IL%'
          AND ca.availability_status IN ('FA', 'W')
          {recency_clause}
          {name_clause}
        ORDER BY p.full_name
        """,
        params or None,
    )
    return cur.fetchall()


def build_user_message(pitcher: dict) -> str:
    name    = pitcher["full_name"]
    team    = pitcher["team"] or "Unknown"
    pos     = pitcher["pos"] or "P"
    il_type = pitcher["il_type"] or "IL"
    avail   = pitcher["avail"]

    lines = [
        f"Player: {name} ({team}, {pos}) — {il_type}, availability: {avail}",
    ]

    if pitcher.get("injury_note"):
        lines.append(f"Injury note from Yahoo: {pitcher['injury_note']}")

    if pitcher["yahoo_status_full"]:
        lines.append(f"Yahoo status detail: {pitcher['yahoo_status_full']}")

    stats_parts = []
    if pitcher["ip"] is not None:
        stats_parts.append(f"IP: {float(pitcher['ip']):.1f}")
    if pitcher["era"] is not None:
        stats_parts.append(f"ERA: {float(pitcher['era']):.2f}")
    if pitcher["whip"] is not None:
        stats_parts.append(f"WHIP: {float(pitcher['whip']):.3f}")
    if pitcher["k"] is not None and pitcher["ip"]:
        k9 = float(pitcher["k"]) * 9.0 / float(pitcher["ip"])
        stats_parts.append(f"K/9: {k9:.1f}")
    if stats_parts:
        lines.append("2026 season stats: " + ", ".join(stats_parts))
    else:
        lines.append("2026 season stats: none (hasn't pitched this season)")

    return "\n".join(lines)


def analyze_pitcher(
    anthropic_client: anthropic.Anthropic,
    pitcher: dict,
) -> dict:
    user_msg = build_user_message(pitcher)
    response = anthropic_client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=1024,
        system=[{
            "type": "text",
            "text": SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }],
        tools=[{**ANALYSIS_TOOL, "cache_control": {"type": "ephemeral"}}],
        tool_choice={"type": "tool", "name": "record_pitcher_analysis"},
        messages=[{"role": "user", "content": user_msg}],
    )
    for block in response.content:
        if block.type == "tool_use" and block.name == "record_pitcher_analysis":
            return block.input
    raise RuntimeError(f"Claude did not call record_pitcher_analysis for {pitcher['full_name']}")


def print_analysis(pitcher: dict, result: dict) -> None:
    note = pitcher.get("injury_note") or "—"
    print(f"\n=== {pitcher['full_name']} ({pitcher['team']}, {pitcher['pos']}) [{pitcher['il_type']}] ===")
    print(f"  Injury note: {note}")
    print(f"  Injury:      {result.get('injury_description')}")
    print(f"  Severity:    {result.get('injury_severity')}/5")
    rd  = result.get("return_date")
    est = result.get("return_date_is_estimate")
    print(f"  Return:      {'~' if est else ''}{rd or 'unknown'}")
    print(f"  Quality:     {'Yes' if result.get('is_high_quality') else 'No'} — {result.get('quality_notes')}")
    print(f"  Return note: {result.get('return_notes')}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    settings = load_settings()
    anthropic_client = anthropic.Anthropic(api_key=load_anthropic_api_key())
    conn = connect(settings)

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    try:
        pitchers = get_fa_il_pitchers(
            conn, player_name=args.player, force=args.force
        )
        if not pitchers:
            print("No FA/W IL pitchers to analyze (all recently analyzed; use --force to override).")
            return 0

        print(f"Analyzing {len(pitchers)} FA/W IL pitcher(s)...")

        for pitcher in pitchers:
            pitcher["injury_note"] = extract_injury_note(pitcher.get("raw_player_xml"))
            result = analyze_pitcher(anthropic_client, pitcher)

            print_analysis(pitcher, result)

            if not args.dry_run:
                upsert_fa_il_analysis(
                    conn,
                    player_id=int(pitcher["player_id"]),
                    injury_description=result.get("injury_description"),
                    injury_severity=result.get("injury_severity"),
                    return_date=result.get("return_date"),
                    return_date_is_estimate=bool(result.get("return_date_is_estimate")),
                    is_high_quality=bool(result.get("is_high_quality")),
                    quality_notes=result.get("quality_notes"),
                    return_notes=result.get("return_notes"),
                    news_sources_json=None,
                    analyzed_at_utc=now,
                )

        if not args.dry_run:
            conn.commit()
            print(f"\nCommitted {len(pitchers)} analysis row(s).")
        else:
            print("\n[Dry run — no DB writes.]")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
