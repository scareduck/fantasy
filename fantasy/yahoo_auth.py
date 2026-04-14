from __future__ import annotations

import base64
import hashlib
import json
import secrets
import sys
import time
import webbrowser
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from fantasy.config import Settings


class InteractiveAuthRequired(RuntimeError):
    """Raised when a new Yahoo OAuth token is needed but no TTY is available."""

AUTH_URL = "https://api.login.yahoo.com/oauth2/request_auth"
TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"


@dataclass
class YahooToken:
    access_token: str
    refresh_token: str
    token_type: str
    expires_at: float

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "YahooToken":
        created_at = float(payload.get("created_at", time.time()))
        expires_in = int(payload.get("expires_in", 3600))
        expires_at = float(payload.get("expires_at", created_at + expires_in - 60))
        return cls(
            access_token=payload["access_token"],
            refresh_token=payload["refresh_token"],
            token_type=payload.get("token_type", "Bearer"),
            expires_at=expires_at,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "expires_at": self.expires_at,
        }

    def is_expired(self, skew_seconds: int = 120) -> bool:
        return time.time() >= (self.expires_at - skew_seconds)


class YahooAuth:
    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or requests.Session()

    def get_valid_token(self) -> YahooToken:
        token = self._load_token()
        if token and not token.is_expired():
            return token
        if token and token.refresh_token:
            refreshed = self._refresh_token(token.refresh_token)
            self._save_token(refreshed)
            return refreshed
        if not sys.stdin.isatty():
            raise InteractiveAuthRequired(
                "Yahoo token is missing or expired and the refresh token is unavailable. "
                "Run `fantasy-sync` interactively to re-authenticate."
            )
        bootstrapped = self._interactive_authorization_code_flow()
        self._save_token(bootstrapped)
        return bootstrapped

    def _load_token(self) -> YahooToken | None:
        path = self.settings.yahoo_token_file
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        return YahooToken.from_payload(payload)

    def _save_token(self, token: YahooToken) -> None:
        path = self.settings.yahoo_token_file
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(token.to_payload(), indent=2), encoding="utf-8")

    def _interactive_authorization_code_flow(self) -> YahooToken:
        state = secrets.token_urlsafe(24)
        code_verifier = secrets.token_urlsafe(64)
        code_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode("ascii")).digest()
        ).rstrip(b"=").decode("ascii")

        params = {
            "client_id": self.settings.yahoo_client_id,
            "redirect_uri": self.settings.yahoo_redirect_uri,
            "response_type": "code",
            "scope": self.settings.yahoo_scope,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        auth_url = f"{AUTH_URL}?{urlencode(params)}"

        print("\nOpen this URL in your browser to authorize Yahoo Fantasy access:\n")
        print(auth_url)
        print()
        try:
            webbrowser.open(auth_url)
        except Exception:
            pass

        parsed = urlparse(self.settings.yahoo_redirect_uri)
        if parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1"):
            port = parsed.port or 80
            code = self._wait_for_local_callback(port, state)
        elif self.settings.yahoo_redirect_uri == "oob":
            code = input("Paste the Yahoo authorization code: ").strip()
        else:
            redirect_value = input(
                "Paste the full redirect URL after approval, or just the code parameter: "
            ).strip()
            code = self._extract_code(redirect_value)

        response = self.session.post(
            TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "client_id": self.settings.yahoo_client_id,
                "redirect_uri": self.settings.yahoo_redirect_uri,
                "code": code,
                "code_verifier": code_verifier,
            },
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"Token exchange failed ({response.status_code}): {response.text}")
        payload = response.json()
        payload["created_at"] = time.time()
        payload["expires_at"] = payload["created_at"] + int(payload.get("expires_in", 3600)) - 60
        return YahooToken.from_payload(payload)

    def _wait_for_local_callback(self, port: int, expected_state: str) -> str:
        captured: dict = {}

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                qs = parse_qs(urlparse(self.path).query)
                captured["code"] = qs.get("code", [None])[0]
                captured["state"] = qs.get("state", [None])[0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<html><body><p>Authorization complete. You may close this tab.</p></body></html>")

            def log_message(self, *args):
                pass  # suppress request logging

        server = HTTPServer(("127.0.0.1", port), _Handler)
        print(f"Waiting for Yahoo callback on port {port} ...")
        server.handle_request()
        server.server_close()

        if captured.get("state") != expected_state:
            raise RuntimeError("OAuth state mismatch — possible CSRF. Re-run to try again.")
        code = captured.get("code")
        if not code:
            raise RuntimeError("No authorization code received in callback.")
        return code

    def _refresh_token(self, refresh_token: str) -> YahooToken:
        response = self.session.post(
            TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "client_id": self.settings.yahoo_client_id,
                "redirect_uri": self.settings.yahoo_redirect_uri,
                "refresh_token": refresh_token,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        payload.setdefault("refresh_token", refresh_token)
        payload["created_at"] = time.time()
        payload["expires_at"] = payload["created_at"] + int(payload.get("expires_in", 3600)) - 60
        return YahooToken.from_payload(payload)

    @staticmethod
    def _extract_code(value: str) -> str:
        if "code=" not in value:
            return value
        for part in value.split("?")[-1].split("&"):
            if part.startswith("code="):
                return part.split("=", 1)[1]
        return value
