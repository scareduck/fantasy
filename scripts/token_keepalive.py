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
        print(f"{now} ERROR: interactive auth required — refresh token has expired")
        return 1
    except Exception as e:
        print(f"{now} ERROR: refresh failed — {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
