#!/usr/bin/env python3
"""One-time Rotowire login helper.

Opens a visible browser window at the Rotowire login page. Log in normally,
then close the browser or wait — the script detects successful login and saves
your session cookies to ~/.rotowire_cookies.json for use by the IL pitcher
analysis scraper.
"""
from __future__ import annotations

import json
import pathlib
import time

from playwright.sync_api import sync_playwright

COOKIES_PATH  = pathlib.Path.home() / ".rotowire_cookies.json"
LOGIN_URL     = "https://www.rotowire.com/subscribe/login/"
POLL_INTERVAL = 1.5   # seconds between URL checks


def main() -> int:
    print("Opening Rotowire login page — please log in with your browser.")
    print("The window will close automatically once you're authenticated.")
    print(f"Cookies will be saved to: {COOKIES_PATH}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page    = context.new_page()
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)

        # Wait until the URL is no longer the login page
        while True:
            try:
                url = page.url
            except Exception:
                break  # browser was closed by user
            if "login" not in url and "subscribe" not in url:
                break
            time.sleep(POLL_INTERVAL)

        try:
            cookies = context.cookies()
        except Exception:
            print("Browser closed before login completed — no cookies saved.")
            return 1

        browser.close()

    COOKIES_PATH.write_text(json.dumps(cookies, indent=2))
    print(f"Saved {len(cookies)} cookies to {COOKIES_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
