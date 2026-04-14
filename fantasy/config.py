from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_PASSWORDS_PATH = Path.home() / ".passwords.json"
_passwords_cache: dict | None = None


def _load_passwords() -> dict:
    global _passwords_cache
    if _passwords_cache is None:
        if _PASSWORDS_PATH.exists():
            with _PASSWORDS_PATH.open() as fh:
                _passwords_cache = json.load(fh)
        else:
            _passwords_cache = {}
    return _passwords_cache


def _pw(key: str, field: str, fallback: Any = "") -> Any:
    """Look up passwords[key][field], returning fallback if absent."""
    return _load_passwords().get(key, {}).get(field, fallback)


@dataclass(frozen=True)
class Settings:
    yahoo_client_id: str
    yahoo_client_secret: str
    yahoo_redirect_uri: str
    yahoo_scope: str
    yahoo_token_file: Path
    yahoo_league_key: str | None
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    snapshot_dir: Path
    local_timezone: str


@dataclass(frozen=True)
class DbSyncSettings:
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    snapshot_dir: Path
    local_timezone: str



def _require(name: str, pw_key: str | None = None, pw_field: str | None = None) -> str:
    value = os.getenv(name, "").strip()
    if not value and pw_key and pw_field:
        value = str(_pw(pw_key, pw_field, "")).strip()
    if not value:
        raise RuntimeError(f"Missing required config: {name} (env) or {pw_key}.{pw_field} (~/.passwords.json)")
    return value



def load_settings() -> Settings:
    return Settings(
        yahoo_client_id=_require("YAHOO_CLIENT_ID", "yahoo-fantasy", "client_id"),
        yahoo_client_secret=_require("YAHOO_CLIENT_SECRET", "yahoo-fantasy", "client_secret"),
        yahoo_redirect_uri=os.getenv("YAHOO_REDIRECT_URI", "").strip() or _pw("yahoo-fantasy", "redirect_uri", "oob"),
        yahoo_scope=os.getenv("YAHOO_SCOPE", "fspt-r").strip() or "fspt-r",
        yahoo_token_file=Path(os.getenv("YAHOO_TOKEN_FILE", "") or Path(__file__).parent.parent / "tokens/yahoo_token.json").expanduser(),
        yahoo_league_key=(os.getenv("YAHOO_LEAGUE_KEY", "").strip() or None),
        db_host=os.getenv("DB_HOST", "").strip() or _pw("fantasy-db", "host", "127.0.0.1"),
        db_port=int(os.getenv("DB_PORT", "") or _pw("fantasy-db", "port", 3306)),
        db_user=_require("DB_USER", "fantasy-db", "user"),
        db_password=os.getenv("DB_PASSWORD", "").strip() or _pw("fantasy-db", "password", ""),
        db_name=_require("DB_NAME", "fantasy-db", "database"),
        snapshot_dir=Path(os.getenv("SNAPSHOT_DIR", "") or Path(__file__).parent.parent / "snapshots").expanduser(),
        local_timezone=os.getenv("LOCAL_TIMEZONE", "America/Chicago").strip() or "America/Chicago",
    )


def load_db_sync_settings() -> DbSyncSettings:
    return DbSyncSettings(
        db_host=os.getenv("DB_HOST", "").strip() or _pw("fantasy-db", "host", "127.0.0.1"),
        db_port=int(os.getenv("DB_PORT", "") or _pw("fantasy-db", "port", 3306)),
        db_user=_require("DB_USER", "fantasy-db", "user"),
        db_password=os.getenv("DB_PASSWORD", "").strip() or _pw("fantasy-db", "password", ""),
        db_name=_require("DB_NAME", "fantasy-db", "database"),
        snapshot_dir=Path(os.getenv("SNAPSHOT_DIR", "") or Path(__file__).parent.parent / "snapshots").expanduser(),
        local_timezone=os.getenv("LOCAL_TIMEZONE", "America/Chicago").strip() or "America/Chicago",
    )
