from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


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



def _require(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value



def load_settings() -> Settings:
    return Settings(
        yahoo_client_id=_require("YAHOO_CLIENT_ID"),
        yahoo_client_secret=_require("YAHOO_CLIENT_SECRET"),
        yahoo_redirect_uri=os.getenv("YAHOO_REDIRECT_URI", "oob").strip() or "oob",
        yahoo_scope=os.getenv("YAHOO_SCOPE", "fspt-r").strip() or "fspt-r",
        yahoo_token_file=Path(os.getenv("YAHOO_TOKEN_FILE", "tokens/yahoo_token.json")).expanduser(),
        yahoo_league_key=(os.getenv("YAHOO_LEAGUE_KEY", "").strip() or None),
        db_host=os.getenv("DB_HOST", "127.0.0.1").strip() or "127.0.0.1",
        db_port=int(os.getenv("DB_PORT", "3306")),
        db_user=_require("DB_USER"),
        db_password=os.getenv("DB_PASSWORD", ""),
        db_name=_require("DB_NAME"),
        snapshot_dir=Path(os.getenv("SNAPSHOT_DIR", "snapshots")).expanduser(),
        local_timezone=os.getenv("LOCAL_TIMEZONE", "America/Chicago").strip() or "America/Chicago",
    )
