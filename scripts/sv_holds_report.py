#!/usr/bin/env python3
"""Email a color-coded last-SV+H-date report for all pitchers with SV+H this season."""
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
  p.subtitle { margin-top: 0; color: #666; font-size: 12px; }
  table { border-collapse: collapse; width: 100%; max-width: 720px; }
  th { background: #2c5f8a; color: #fff; padding: 7px 10px; text-align: left; }
  td { padding: 6px 10px; border-bottom: 1px solid #ddd; }
  .num { text-align: right; }
  tr.svh-green td { background-color: #c3e6cb; }
  tr.svh-amber td { background-color: #ffeeba; }
  tr.svh-red   td { background-color: #f5c6cb; }
</style>
"""

COLS = [
    ("full_name",     "Pitcher"),
    ("team",          "Team"),
    ("avail_status",  "Status"),
    ("season_svh",    "SV+H"),
    ("last_svh_date", "Last SV+H"),
    ("days_since",    "Days Ago"),
]

QUERY = """
SELECT
    p.full_name,
    p.editorial_team_abbr                   AS team,
    p.yahoo_player_key,
    CAST(cps.sv_holds AS UNSIGNED)          AS season_svh,
    MAX(pgl.game_date)                      AS last_svh_date,
    DATEDIFF(CURDATE(), MAX(pgl.game_date)) AS days_since,
    COALESCE(ca.availability_status, 'T')   AS avail_status
FROM current_pitcher_stats cps
JOIN  player p ON p.player_id = cps.player_id
LEFT JOIN pitcher_game_log pgl ON pgl.player_id = cps.player_id
LEFT JOIN (
    SELECT player_id, MAX(availability_status) AS availability_status
    FROM   current_availability
    GROUP BY player_id
) ca ON ca.player_id = cps.player_id
WHERE cps.sv_holds >= 1
GROUP BY
    p.player_id, p.full_name, p.editorial_team_abbr,
    p.yahoo_player_key, cps.sv_holds, ca.availability_status
ORDER BY
    CASE WHEN MAX(pgl.game_date) IS NULL THEN 1 ELSE 0 END,
    DATEDIFF(CURDATE(), MAX(pgl.game_date)),
    cps.sv_holds DESC
"""


def row_class(days_since) -> str:
    if days_since is None:
        return "svh-red"
    d = int(days_since)
    if d <= 5:
        return "svh-green"
    if d <= 10:
        return "svh-amber"
    return "svh-red"


def yahoo_stats_url(player_key: str) -> str:
    numeric_id = player_key.rsplit(".", 1)[-1]
    return f"https://sports.yahoo.com/mlb/players/{numeric_id}/"


def build_html(rows: list[dict]) -> str:
    today = date.today().strftime("%B %d, %Y")
    headers = "".join(f"<th>{label}</th>" for _, label in COLS)
    body_rows = []
    for r in rows:
        cls = row_class(r.get("days_since"))
        cells = []
        for col_key, _ in COLS:
            val = r.get(col_key)
            if col_key == "full_name":
                key = r.get("yahoo_player_key") or ""
                url = yahoo_stats_url(key) if key else "#"
                cells.append(f'<td><a href="{url}">{val}</a></td>')
            elif col_key == "days_since":
                display = str(int(val)) if val is not None else "?"
                cells.append(f'<td class="num">{display}</td>')
            elif col_key == "last_svh_date":
                cells.append(f"<td>{val if val else 'no data'}</td>")
            elif col_key == "season_svh":
                cells.append(f'<td class="num">{val if val is not None else ""}</td>')
            else:
                cells.append(f"<td>{val if val is not None else ''}</td>")
        body_rows.append(f'  <tr class="{cls}">{"".join(cells)}</tr>')

    table = (
        f"<table><tr>{headers}</tr>\n"
        + "\n".join(body_rows)
        + "\n</table>"
    )
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">{HTML_STYLE}</head>
<body>
<h2>Pitcher SV+H Tracker</h2>
<p class="subtitle">Generated {today} &mdash;
  <span style="background:#c3e6cb;padding:1px 6px;">green</span> 1&ndash;5 days ago &nbsp;
  <span style="background:#ffeeba;padding:1px 6px;">amber</span> 6&ndash;10 &nbsp;
  <span style="background:#f5c6cb;padding:1px 6px;">red</span> 11+ or no log data
</p>
{table}
</body>
</html>"""


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
    parser = argparse.ArgumentParser(description="Generate and email the pitcher SV+H tracker report.")
    parser.add_argument("--email", action="store_true", help="Send report via email.")
    parser.add_argument("--recipients", nargs="+", metavar="NAME", help="Limit recipients from reports.json.")
    parser.add_argument("--html-dir", default=None, help="Directory to write the HTML file into.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_db_sync_settings()
    conn = connect(settings)
    cur = conn.cursor(dictionary=True)
    cur.execute(QUERY)
    rows = cur.fetchall()
    conn.close()

    html = build_html(rows)

    if args.html_dir:
        out_dir = Path(args.html_dir).expanduser()
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "sv_holds.html"
        out_path.write_text(html, encoding="utf-8")
        print(f"Wrote {out_path}")

    if not args.email:
        return 0

    smtp_account = "rlm@scareduck.com"
    smtp_host_raw = _pw(smtp_account, "smtp_host", "")
    if not smtp_host_raw:
        raise SystemExit(f"No smtp_host for {smtp_account!r} in ~/.passwords.json")
    if ":" in smtp_host_raw:
        smtp_host, smtp_port_str = smtp_host_raw.rsplit(":", 1)
        smtp_port = int(smtp_port_str)
    else:
        smtp_host = smtp_host_raw
        smtp_port = int(_pw(smtp_account, "smtp_port", 587))
    smtp_password = _pw(smtp_account, "password", "")

    config_path = REPO_ROOT / "reports.json"
    recipients: list[dict] = json.loads(config_path.read_text(encoding="utf-8"))
    if args.recipients:
        recipients = [r for r in recipients if r["name"] in args.recipients]
        if not recipients:
            raise SystemExit(f"No matching recipients in reports.json: {args.recipients}")

    subject = f"Pitcher SV+H Tracker ({date.today().strftime('%b %d')})"
    for recipient in recipients:
        email_addr = recipient.get("email", "")
        if not email_addr:
            print(f"WARNING: no email for {recipient['name']}, skipping.")
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

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
