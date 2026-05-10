#!/usr/bin/env python3
"""Generate a combined waiver-wire pitcher report (top_2 + top_starts) and email it."""
from __future__ import annotations

import argparse
import json
import smtplib
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from fantasy.config import _pw, load_db_sync_settings
from fantasy.db import connect

REPO_ROOT = Path(__file__).parent.parent

HTML_STYLE = """
<style>
  body { font-family: Arial, sans-serif; font-size: 14px; color: #222; }
  h2 { margin-bottom: 4px; }
  h3 { margin-top: 24px; margin-bottom: 4px; }
  p.subtitle { margin-top: 0; color: #666; font-size: 12px; }
  table { border-collapse: collapse; width: 100%; max-width: 720px; margin-bottom: 16px; }
  th { background: #2c5f8a; color: #fff; padding: 7px 10px; text-align: left; }
  td { padding: 6px 10px; border-bottom: 1px solid #ddd; }
  tr:nth-child(even) td { background: #f4f8fc; }
  .fpts { text-align: right; font-weight: bold; }
  .fpts-high { color: #1a7a1a; }
  .fpts-low  { color: #999; }
</style>
"""

TOP_2_COLS = [
    ("full_name",   "Pitcher"),
    ("team",        "Team"),
    ("week",        "Week"),
    ("total_fpts",  "FPTS"),
    ("starts",      "Starts"),
    ("era",         "ERA"),
]

TOP_STARTS_COLS = [
    ("full_name",   "Pitcher"),
    ("team",        "Team"),
    ("week",        "Week"),
    ("fpts",        "FPTS"),
    ("starts",      "Start"),
    ("era",         "ERA"),
]

FPTS_KEYS = {"total_fpts", "fpts"}


def fpts_class(fpts: float) -> str:
    if fpts >= 11:
        return "fpts fpts-high"
    if fpts < 6:
        return "fpts fpts-low"
    return "fpts"


def fetch_league_id(conn) -> str | None:
    cur = conn.cursor()
    cur.execute("SELECT yahoo_league_key FROM league LIMIT 1")
    row = cur.fetchone()
    return row[0].rsplit(".", 1)[-1] if row else None


def yahoo_stats_url(player_key: str) -> str:
    numeric_id = player_key.rsplit(".", 1)[-1]
    return f"https://sports.yahoo.com/mlb/players/{numeric_id}/"


def yahoo_add_url(player_key: str, league_id: str) -> str:
    numeric_id = player_key.rsplit(".", 1)[-1]
    return f"https://baseball.fantasysports.yahoo.com/b1/{league_id}/addplayer?apid={numeric_id}"


def player_cell(r: dict, league_id: str | None = None) -> str:
    key = r.get("yahoo_player_key") or ""
    name = r["full_name"]
    if not key:
        return name
    stats_url = yahoo_stats_url(key)
    if league_id:
        add_url = yahoo_add_url(key, league_id)
        return f'<a href="{add_url}">{name}</a> <a href="{stats_url}" style="font-size: 11px; color: #666;">Stats</a>'
    return f'<a href="{stats_url}">{name}</a>'


def table_html(rows: list[dict], cols: list[tuple[str, str]], league_id: str | None = None) -> str:
    headers = "".join(f"<th>{label}</th>" for _, label in cols)
    body_rows = []
    for r in rows:
        cells = []
        for col_key, _ in cols:
            val = r.get(col_key, "")
            if col_key == "full_name":
                cells.append(f"<td>{player_cell(r, league_id)}</td>")
            elif col_key in FPTS_KEYS:
                fpts = float(val)
                cls = fpts_class(fpts)
                cells.append(f"<td class='{cls}'>{fpts:.1f}</td>")
            else:
                cells.append(f"<td>{val}</td>")
        body_rows.append(f"  <tr>{''.join(cells)}</tr>")
    return f"<table><tr>{headers}</tr>\n" + "\n".join(body_rows) + "\n</table>"


def build_html(top2_rows: list[dict], top_starts_rows: list[dict], league_id: str | None = None) -> str:
    today = date.today().strftime("%B %d, %Y")
    t2 = table_html(top2_rows, TOP_2_COLS, league_id)
    ts = table_html(top_starts_rows, TOP_STARTS_COLS, league_id)
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">{HTML_STYLE}</head>
<body>
<h2>Waiver Wire Pitchers</h2>
<p class="subtitle">Generated {today}</p>
<h3>Top Free Agents, Two Starts</h3>
{t2}
<h3>Top Free Agent Single Starts</h3>
{ts}
</body>
</html>"""


def run_sql(conn, sql_path: Path) -> list[dict]:
    cur = conn.cursor(dictionary=True)
    cur.execute(sql_path.read_text(encoding="utf-8"))
    return cur.fetchall()


def send_email(
    *,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    from_addr: str,
    to_addr: str,
    subject: str,
    html_body: str,
) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    if smtp_port == 465:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
            server.login(smtp_user, smtp_password)
            server.sendmail(from_addr, [to_addr], msg.as_string())
    else:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(from_addr, [to_addr], msg.as_string())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and/or email the waiver-wire pitcher report.")
    parser.add_argument("--html-dir", default=None, help="Directory to write the HTML file into.")
    parser.add_argument("--email", action="store_true", help="Send report via email.")
    parser.add_argument(
        "--recipients",
        nargs="+",
        metavar="NAME",
        help="Limit recipients from reports.json (default: all). E.g. --recipients rob helen",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_db_sync_settings()
    conn = connect(settings)

    league_id = fetch_league_id(conn)
    top2_rows = run_sql(conn, REPO_ROOT / "top_2.sql")
    top_starts_rows = run_sql(conn, REPO_ROOT / "top_starts.sql")
    conn.close()

    html = build_html(top2_rows, top_starts_rows, league_id)

    html_dir = Path(args.html_dir).expanduser() if args.html_dir else None
    if html_dir:
        html_dir.mkdir(parents=True, exist_ok=True)
        out_path = html_dir / "waiver_pitchers.html"
        out_path.write_text(html, encoding="utf-8")
        print(f"Wrote {out_path}")

    if args.email:
        smtp_account = "rlm@scareduck.com"
        smtp_host_raw = _pw(smtp_account, "smtp_host", "")
        if not smtp_host_raw:
            raise SystemExit(f"Email requested but no '{smtp_account}' smtp_host found in ~/.passwords.json.")
        if ":" in smtp_host_raw:
            smtp_host, smtp_port_str = smtp_host_raw.rsplit(":", 1)
            smtp_port = int(smtp_port_str)
        else:
            smtp_host = smtp_host_raw
            smtp_port = int(_pw(smtp_account, "smtp_port", 587))
        smtp_password = _pw(smtp_account, "password", "")

        config_path = REPO_ROOT / "reports.json"
        if not config_path.exists():
            raise SystemExit(f"Recipient config not found: {config_path}")
        recipients: list[dict] = json.loads(config_path.read_text(encoding="utf-8"))
        if args.recipients:
            recipients = [r for r in recipients if r["name"] in args.recipients]
            if not recipients:
                raise SystemExit(f"None of the requested recipients found in reports.json: {args.recipients}")

        subject = f"Waiver Wire Pitchers ({date.today().strftime('%b %d')})"
        for recipient in recipients:
            email_addr = recipient.get("email", "")
            if not email_addr:
                print(f"WARNING: no email address for {recipient['name']}, skipping.")
                continue
            send_email(
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                smtp_user=smtp_account,
                smtp_password=smtp_password,
                from_addr=smtp_account,
                to_addr=email_addr,
                subject=subject,
                html_body=html,
            )
            print(f"Emailed {recipient['name']} <{email_addr}>")

    if not html_dir and not args.email:
        print(html)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
