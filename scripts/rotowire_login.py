#!/usr/bin/env python3
"""Rotowire cookie helper.

Preferred: extracts cookies directly from your Firefox profile (fastest, no
browser window needed). Falls back to opening a headed Playwright window if
Firefox cookies are not found.

Saves cookies to ~/.rotowire_cookies.json for use by fantasy-il-pitchers.
"""
from __future__ import annotations

import json
import pathlib
import shutil
import sqlite3
import tempfile
import time

COOKIES_PATH   = pathlib.Path.home() / ".rotowire_cookies.json"
FIREFOX_PROFILE = pathlib.Path.home() / ".mozilla/firefox"
LOGIN_URL      = "https://www.rotowire.com/subscribe/login/"


def find_firefox_cookies() -> pathlib.Path | None:
    for db in sorted(FIREFOX_PROFILE.rglob("cookies.sqlite")):
        return db
    return None


def extract_firefox_cookies(db_path: pathlib.Path) -> list[dict]:
    tmp = pathlib.Path(tempfile.mktemp(suffix=".sqlite"))
    shutil.copy2(db_path, tmp)
    try:
        conn = sqlite3.connect(tmp)
        rows = conn.execute(
            "SELECT name, value, host, path, expiry, isSecure, isHttpOnly, sameSite "
            "FROM moz_cookies WHERE host LIKE '%rotowire%'"
        ).fetchall()
        conn.close()
    finally:
        tmp.unlink(missing_ok=True)

    cookies = []
    for name, value, host, path, expiry, secure, httponly, samesite in rows:
        cookies.append({
            "name": name, "value": value,
            "domain": host, "path": path,
            "expires": int(expiry / 1000) if expiry > 0 else -1,
            "secure": bool(secure),
            "httpOnly": bool(httponly),
            "sameSite": ["None", "Lax", "Strict"][samesite] if samesite in (0, 1, 2) else "Lax",
        })
    return cookies


def extract_via_browser() -> list[dict]:
    from playwright.sync_api import sync_playwright

    print("Opening Rotowire login page — please log in, then wait.")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page    = context.new_page()
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)

        while True:
            try:
                url = page.url
            except Exception:
                break
            if "login" not in url and "subscribe" not in url:
                break
            time.sleep(1.5)

        try:
            cookies = context.cookies()
        except Exception:
            print("Browser closed before login completed.")
            return []
        browser.close()

    # Convert Playwright cookie format (already correct)
    return cookies


def main() -> int:
    ff_db = find_firefox_cookies()
    if ff_db:
        print(f"Found Firefox cookie store: {ff_db}")
        cookies = extract_firefox_cookies(ff_db)
        rw = [c for c in cookies if "rotowire" in c["domain"]]
        if rw:
            COOKIES_PATH.write_text(json.dumps(rw, indent=2))
            print(f"Saved {len(rw)} Rotowire cookies from Firefox to {COOKIES_PATH}")
            return 0
        print("No Rotowire cookies found in Firefox — falling back to browser login.")

    cookies = extract_via_browser()
    if not cookies:
        return 1

    COOKIES_PATH.write_text(json.dumps(cookies, indent=2))
    print(f"Saved {len(cookies)} cookies to {COOKIES_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
