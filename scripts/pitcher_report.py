#!/usr/bin/env python3
"""Generate pitcher start reports as HTML and/or email them to recipients."""
from __future__ import annotations

import argparse
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
  table { border-collapse: collapse; width: 100%; max-width: 680px; }
  th { background: #2c5f8a; color: #fff; padding: 7px 10px; text-align: left; }
  td { padding: 6px 10px; border-bottom: 1px solid #ddd; }
  tr:nth-child(even) td { background: #f4f8fc; }
  .fpts { text-align: right; font-weight: bold; }
  .fpts-high { color: #1a7a1a; }
  .fpts-low  { color: #999; }
</style>
"""


def fpts_class(fpts: float) -> str:
    if fpts >= 11:
        return "fpts fpts-high"
    if fpts < 6:
        return "fpts fpts-low"
    return "fpts"


def rows_to_html(rows: list[dict], title: str) -> str:
    today = date.today().strftime("%B %d, %Y")
    body_rows = []
    for r in rows:
        fpts = float(r["fpts"])
        cls = fpts_class(fpts)
        body_rows.append(
            f"  <tr>"
            f"<td>{r['start_date']}</td>"
            f"<td>{r['full_name']}</td>"
            f"<td>{r['team']}</td>"
            f"<td>{r['slot']}</td>"
            f"<td>{r['start']}</td>"
            f"<td class='{cls}'>{fpts:.1f}</td>"
            f"</tr>"
        )
    table = "\n".join(body_rows)
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">{HTML_STYLE}</head>
<body>
<h2>{title}</h2>
<p class="subtitle">Generated {today}</p>
<table>
  <tr>
    <th>Date</th><th>Pitcher</th><th>MLB Team</th>
    <th>Slot</th><th>Matchup</th><th>FPTS</th>
  </tr>
{table}
</table>
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and/or email pitcher start reports.")
    parser.add_argument("--html-dir", default=None, help="Directory to write HTML files into.")
    parser.add_argument("--email", action="store_true", help="Send reports via email.")
    parser.add_argument(
        "--recipients",
        default=None,
        nargs="+",
        help="Recipient names to include (default: all configured). E.g. --recipients rob helen",
    )
    return parser.parse_args()


def load_recipients(requested: list[str] | None) -> list[dict]:
    """Load recipient config from reports.json in the repo root."""
    config_path = REPO_ROOT / "reports.json"
    if not config_path.exists():
        raise SystemExit(f"Recipient config not found: {config_path}")
    import json
    recipients: list[dict] = json.loads(config_path.read_text(encoding="utf-8"))
    if requested:
        recipients = [r for r in recipients if r["name"] in requested]
        if not recipients:
            raise SystemExit(f"None of the requested recipients found in reports.json: {requested}")
    return recipients


def main() -> int:
    args = parse_args()
    settings = load_db_sync_settings()
    conn = connect(settings)

    html_dir = Path(args.html_dir).expanduser() if args.html_dir else None
    if html_dir:
        html_dir.mkdir(parents=True, exist_ok=True)

    smtp_account = "rlm@scareduck.com"
    smtp_host_raw = _pw(smtp_account, "smtp_host", "")
    if ":" in smtp_host_raw:
        smtp_host, smtp_port_str = smtp_host_raw.rsplit(":", 1)
        smtp_port = int(smtp_port_str)
    else:
        smtp_host = smtp_host_raw
        smtp_port = int(_pw(smtp_account, "smtp_port", 587))
    smtp_user = smtp_account
    smtp_password = _pw(smtp_account, "password", "")
    smtp_from = smtp_account

    if args.email and not smtp_host:
        raise SystemExit(f"Email requested but no '{smtp_account}' smtp_host found in ~/.passwords.json.")

    recipients = load_recipients(args.recipients)

    for recipient in recipients:
        name = recipient["name"]
        email = recipient.get("email", "")
        sql_path = REPO_ROOT / recipient["sql"]
        title = recipient.get("title", f"Pitcher Starts — {name.title()}")

        if not sql_path.exists():
            print(f"WARNING: SQL file not found for {name}: {sql_path}")
            continue

        rows = run_sql(conn, sql_path)
        html = rows_to_html(rows, title)

        if html_dir:
            out_path = html_dir / f"{name}_starts.html"
            out_path.write_text(html, encoding="utf-8")
            print(f"Wrote {out_path}")

        if args.email:
            if not email:
                print(f"WARNING: no email address for {name}, skipping.")
                continue
            subject = f"{title} ({date.today().strftime('%b %d')})"
            send_email(
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                smtp_user=smtp_user,
                smtp_password=smtp_password,
                from_addr=smtp_from,
                to_addr=email,
                subject=subject,
                html_body=html,
            )
            print(f"Emailed {name} <{email}>")

        if not html_dir and not args.email:
            print(html)

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
