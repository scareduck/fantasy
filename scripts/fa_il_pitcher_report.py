#!/usr/bin/env python3
"""Identify FA/waiver IL pitchers and use Claude to score injury severity,
estimate return dates, and flag high-quality targets.
"""
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from datetime import date, datetime, timezone
from urllib.request import urlopen
from xml.etree import ElementTree as ET

import anthropic
import mariadb
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from fantasy.config import load_anthropic_api_key, load_settings
from fantasy.db import connect, upsert_fa_il_analysis
from fantasy.yahoo_xml import NS, find_text

MLB_TRANSACTIONS_URL  = "https://statsapi.mlb.com/api/v1/transactions"
ESPN_INJURIES_URL     = "https://www.espn.com/mlb/injuries"
ESPN_USER_AGENT       = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

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


def _normalize_name(name: str) -> str:
    """Lowercase, strip accents, collapse whitespace for fuzzy name matching."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = nfkd.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", ascii_name).strip().lower()


def _fetch_txn_chunk(start: str, end: str) -> list[dict]:
    url = f"{MLB_TRANSACTIONS_URL}?startDate={start}&endDate={end}&sportId=1&limit=2000"
    with urlopen(url, timeout=30) as resp:
        return json.load(resp).get("transactions", [])


def fetch_il_transactions(season: int) -> dict[str, list[str]]:
    """Return a map of normalized player name -> list of IL placement descriptions
    for the given season. Fetches month-by-month to avoid the 2000-record API cap.
    """
    today    = date.today()
    result: dict[str, list[str]] = {}
    month_start = date(season, 3, 1)  # MLB season starts in March

    while month_start <= today:
        next_month  = date(month_start.year + (month_start.month // 12),
                           (month_start.month % 12) + 1, 1)
        chunk_end   = min(next_month - __import__("datetime").timedelta(days=1), today)
        txns = _fetch_txn_chunk(month_start.isoformat(), chunk_end.isoformat())
        for txn in txns:
            if txn.get("typeDesc") != "Status Change":
                continue
            desc = txn.get("description", "")
            if "injured list" not in desc.lower() or "activated" in desc.lower():
                continue
            name = txn.get("person", {}).get("fullName", "")
            if not name:
                continue
            result.setdefault(_normalize_name(name), []).append(desc)
        month_start = next_month

    return result


def fetch_espn_injury_notes() -> dict[str, str]:
    """Scrape ESPN's MLB injury page and return a map of normalized player name
    -> latest update text (e.g. 'Jun 7: Verlander (hip) will not return...')
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent=ESPN_USER_AGENT)
        page.goto(ESPN_INJURIES_URL, wait_until="domcontentloaded", timeout=60_000)
        try:
            page.wait_for_selector("table", timeout=15_000)
        except Exception:
            pass
        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")
    result: dict[str, str] = {}
    for row in soup.select("tr.Table__TR"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        name = cells[0].get_text(strip=True)
        note = cells[4].get_text(strip=True)
        if name and note:
            result[_normalize_name(name)] = note
    return result


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


def build_user_message(pitcher: dict, il_txns: dict[str, list[str]],
                        espn_notes: dict[str, str]) -> str:
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

    espn_note = espn_notes.get(_normalize_name(name))
    if espn_note:
        lines.append(f"ESPN injury update: {espn_note}")

    txn_descs = il_txns.get(_normalize_name(name), [])
    if txn_descs:
        lines.append(f"MLB transaction record(s):")
        for desc in txn_descs[-3:]:  # most recent up to 3
            lines.append(f"  - {desc}")

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
    il_txns: dict[str, list[str]],
    espn_notes: dict[str, str],
) -> dict:
    user_msg = build_user_message(pitcher, il_txns, espn_notes)
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
        print("Fetching MLB IL transactions...")
        il_txns = fetch_il_transactions(datetime.now(timezone.utc).year)
        print(f"Found {sum(len(v) for v in il_txns.values())} IL placement records for {len(il_txns)} players")

        print("Fetching ESPN injury updates...")
        espn_notes = fetch_espn_injury_notes()
        print(f"Found ESPN notes for {len(espn_notes)} players")

        for pitcher in pitchers:
            pitcher["injury_note"] = extract_injury_note(pitcher.get("raw_player_xml"))
            result = analyze_pitcher(anthropic_client, pitcher, il_txns, espn_notes)

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
