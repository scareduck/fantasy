#!/usr/bin/env python3
"""Identify FA/waiver IL pitchers and use Claude to score injury severity,
estimate return dates, and flag high-quality targets.
"""
from __future__ import annotations

import argparse
import json
import pathlib
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
from fantasy.db import (
    connect,
    insert_rotowire_injury_snapshot,
    load_rotowire_injuries,
    upsert_fa_il_analysis,
)
from fantasy.yahoo_xml import NS, find_text

MLB_TRANSACTIONS_URL  = "https://statsapi.mlb.com/api/v1/transactions"
ESPN_INJURIES_URL     = "https://www.espn.com/mlb/injuries"
ROTOWIRE_INJURIES_URL = "https://www.rotowire.com/baseball/tables/injury-report.php?team=ALL&pos=ALL&league=ALL"
ROTOWIRE_COOKIES_PATH = pathlib.Path.home() / ".rotowire_cookies.json"
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
  A specific date reported by a source (e.g. Rotowire's expected-return
  date, or a specific date mentioned in a news update) always takes
  precedence, even for Tommy John surgery or UCL repair/reconstruction —
  do not override a reported date with a generic recovery-timeline rule.
  For vague language ("late June" → June 26, "mid-July" → July 15,
  "early August" → August 5), infer a date and set
  return_date_is_estimate = true.
  Only when no source gives any date at all: for Tommy John surgery or
  UCL repair/reconstruction, estimate 18 months from the surgery date
  (use the transaction or news date if available, otherwise today's date),
  and set return_date_is_estimate = true.
  Set return_date = null only if there is genuinely no information and the
  injury is not Tommy John/UCL surgery.

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
                "description": "Brief description of the injury and current rehab status if known (1-2 sentences max). Include rehab starts, simulated games, or ramp-up activity when mentioned in the news.",
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
    parser.add_argument("--rotowire", action="store_true",
                        help="Fetch Rotowire injury notes (requires fantasy-rotowire-login).")
    parser.add_argument("--cached-notes", action="store_true",
                        help="Use cached Rotowire notes from DB (any age) and skip ESPN scrape.")
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


def fetch_il_transactions(season: int) -> dict[str, list[tuple[str, str]]]:
    """Return a map of normalized player name -> list of (date, description)
    IL placement/rehab records for the given season, oldest first. Fetches
    month-by-month to avoid the 2000-record API cap.
    """
    today    = date.today()
    result: dict[str, list[tuple[str, str]]] = {}
    month_start = date(season, 3, 1)  # MLB season starts in March

    while month_start <= today:
        next_month  = date(month_start.year + (month_start.month // 12),
                           (month_start.month % 12) + 1, 1)
        chunk_end   = min(next_month - __import__("datetime").timedelta(days=1), today)
        txns = _fetch_txn_chunk(month_start.isoformat(), chunk_end.isoformat())
        for txn in txns:
            desc = txn.get("description", "")
            type_desc = txn.get("typeDesc", "")
            is_il = type_desc == "Status Change" and "injured list" in desc.lower() and "activated" not in desc.lower()
            is_rehab = type_desc == "Assigned" and "rehab assignment" in desc.lower()
            if not (is_il or is_rehab):
                continue
            name = txn.get("person", {}).get("fullName", "")
            if not name:
                continue
            result.setdefault(_normalize_name(name), []).append((txn.get("date", ""), desc))
        month_start = next_month

    return result


def _rotowire_rows_to_notes(rows: list[dict]) -> dict[str, str]:
    result: dict[str, str] = {}
    for p in rows:
        name   = p.get("player_name") or p.get("player", "")
        injury = p.get("injury", "") or ""
        status = p.get("status", "") or ""
        r_date = p.get("r_date") or p.get("rDate", "") or ""
        if not name:
            continue
        note = injury
        if status:
            note += f" — {status}"
        if r_date:
            note += f" — exp. {r_date}"
        result[_normalize_name(name)] = note
    return result


def fetch_rotowire_injury_notes(conn) -> dict[str, str]:
    """Return Rotowire injury notes, using DB cache when fresh enough.

    Checks the DB for a snapshot < 12 hours old first. If stale or absent,
    fetches from Rotowire, persists to DB, and returns fresh notes.
    Falls back to {} if cookies are missing or the session has expired.
    """
    cached_rows, cached_at = load_rotowire_injuries(conn)
    if cached_rows:
        print(f"  Using cached Rotowire snapshot from {cached_at}")
        return _rotowire_rows_to_notes(cached_rows)

    if not ROTOWIRE_COOKIES_PATH.exists():
        print("  No Rotowire cookies found — run fantasy-rotowire-login first.")
        return {}

    cookies = json.loads(ROTOWIRE_COOKIES_PATH.read_text())
    cookie_dict = {c["name"]: c["value"] for c in cookies}

    import requests as _requests
    session = _requests.Session()
    session.headers.update({"User-Agent": ESPN_USER_AGENT,
                             "Referer": "https://www.rotowire.com/baseball/injury-report.php"})
    session.cookies.update(cookie_dict)

    resp = session.get(ROTOWIRE_INJURIES_URL, timeout=20)
    if resp.status_code != 200:
        print(f"  Rotowire returned HTTP {resp.status_code} — session may have expired.")
        return {}

    try:
        players = resp.json()
    except Exception:
        print("  Rotowire response was not JSON — session may have expired.")
        return {}

    captured_at = datetime.now(timezone.utc).replace(tzinfo=None)
    for p in players:
        name = p.get("player", "")
        if not name:
            continue
        insert_rotowire_injury_snapshot(
            conn,
            captured_at_utc=captured_at,
            rotowire_player_id=int(p["ID"]) if p.get("ID") else None,
            player_name=name,
            team=p.get("team") or None,
            position=p.get("position") or None,
            injury=p.get("injury") or None,
            status=p.get("status") or None,
            r_date=p.get("rDate") or None,
            rotowire_url=p.get("URL") or None,
        )
    conn.commit()
    print(f"  Stored {len(players)} Rotowire records in DB")

    # Normalise keys to match _rotowire_rows_to_notes
    raw_rows = [{"player_name": p.get("player",""), "injury": p.get("injury",""),
                 "status": p.get("status",""), "r_date": p.get("rDate","")}
                for p in players]
    return _rotowire_rows_to_notes(raw_rows)


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


def build_user_message(pitcher: dict, il_txns: dict[str, list[tuple[str, str]]],
                        espn_notes: dict[str, str],
                        rw_notes: dict[str, str] | None = None) -> str:
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

    key = _normalize_name(name)
    rw_note = (rw_notes or {}).get(key)
    espn_note = espn_notes.get(key)
    if rw_note:
        lines.append(f"Injury update (Rotowire): {rw_note}")
    if espn_note:
        lines.append(f"Injury update (ESPN): {espn_note}")

    txns = il_txns.get(key, [])
    if txns:
        lines.append(f"MLB transaction record(s), oldest to most recent:")
        for txn_date, desc in txns[-3:]:  # most recent up to 3
            lines.append(f"  - {txn_date}: {desc}")

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
    il_txns: dict[str, list[tuple[str, str]]],
    espn_notes: dict[str, str],
    rw_notes: dict[str, str] | None = None,
) -> dict:
    user_msg = build_user_message(pitcher, il_txns, espn_notes, rw_notes)
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

        if args.cached_notes:
            print("Loading cached Rotowire notes from DB (skipping live scrapes)...")
            cached_rows, cached_at = load_rotowire_injuries(conn, max_age_hours=999999)
            rw_notes = _rotowire_rows_to_notes(cached_rows) if cached_rows else {}
            print(f"Found cached Rotowire notes for {len(rw_notes)} players (snapshot: {cached_at})")
            espn_notes = {}
        else:
            if args.rotowire:
                print("Fetching Rotowire injury notes...")
                rw_notes = fetch_rotowire_injury_notes(conn)
                print(f"Found Rotowire notes for {len(rw_notes)} players")
            else:
                rw_notes = {}

            print("Fetching ESPN injury updates...")
            espn_notes = fetch_espn_injury_notes()
            print(f"Found ESPN notes for {len(espn_notes)} players")

        for pitcher in pitchers:
            pitcher["injury_note"] = extract_injury_note(pitcher.get("raw_player_xml"))
            result = analyze_pitcher(anthropic_client, pitcher, il_txns, espn_notes, rw_notes)

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
