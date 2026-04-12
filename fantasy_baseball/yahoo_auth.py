from __future__ import annotations

import json
import secrets
import time
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

from fantasy_baseball.config import Settings

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
        nonce = secrets.token_urlsafe(24)
        params = {
            "client_id": self.settings.yahoo_client_id,
            "redirect_uri": self.settings.yahoo_redirect_uri,
            "response_type": "code",
            "scope": self.settings.yahoo_scope,
            "state": state,
            "nonce": nonce,
        }
        auth_url = f"{AUTH_URL}?{urlencode(params)}"

        print("\nOpen this URL in your browser to authorize Yahoo Fantasy access:\n")
        print(auth_url)
        print()
        try:
            webbrowser.open(auth_url)
        except Exception:
            pass

        if self.settings.yahoo_redirect_uri == "oob":
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
                "redirect_uri": self.settings.yahoo_redirect_uri,
                "code": code,
            },
            auth=(self.settings.yahoo_client_id, self.settings.yahoo_client_secret),
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        payload["created_at"] = time.time()
        payload["expires_at"] = payload["created_at"] + int(payload.get("expires_in", 3600)) - 60
        return YahooToken.from_payload(payload)

    def _refresh_token(self, refresh_token: str) -> YahooToken:
        response = self.session.post(
            TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "redirect_uri": self.settings.yahoo_redirect_uri,
                "refresh_token": refresh_token,
            },
            auth=(self.settings.yahoo_client_id, self.settings.yahoo_client_secret),
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
