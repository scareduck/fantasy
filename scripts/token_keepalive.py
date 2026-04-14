#!/usr/bin/env python3
"""
Refresh the Yahoo OAuth token and log the result.
Intended to be run frequently (e.g. every 30 minutes) to determine
how long Yahoo refresh tokens survive before requiring re-authentication.
"""
from __future__ import annotations

import sys
from datetime import datetime

from fantasy.config import load_settings
from fantasy.notify import send_alert
from fantasy.yahoo_auth import InteractiveAuthRequired, YahooAuth


def main() -> int:
    settings = load_settings()
    auth = YahooAuth(settings)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    token = auth._load_token()
    if token is None:
        print(f"{now} ERROR: no token file found — run fantasy-sync to authenticate first")
        return 1

    try:
        refreshed = auth._refresh_token(token.refresh_token)
        auth._save_token(refreshed)
        expires = datetime.fromtimestamp(refreshed.expires_at).strftime("%Y-%m-%d %H:%M:%S")
        print(f"{now} OK: token refreshed, new expires_at={expires}")
        return 0
    except InteractiveAuthRequired:
        msg = "Yahoo refresh token has expired. Run `fantasy-sync` interactively to re-authenticate."
        print(f"{now} ERROR: {msg}", file=sys.stderr)
        _alert(f"⚠️ Fantasy Baseball: Yahoo re-authentication required", msg)
        return 1
    except Exception as e:
        msg = f"Yahoo token refresh failed: {e}"
        print(f"{now} ERROR: {msg}", file=sys.stderr)
        _alert("⚠️ Fantasy Baseball: token refresh error", msg)
        return 1


def _alert(subject: str, body: str) -> None:
    try:
        send_alert(subject, body)
        print(f"Alert email sent.", file=sys.stderr)
    except Exception as mail_err:
        print(f"Could not send alert email: {mail_err}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
