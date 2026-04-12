from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo



def utc_now() -> datetime:
    return datetime.now(timezone.utc)



def format_snapshot_timestamp(dt_utc: datetime, tz_name: str) -> str:
    local_dt = dt_utc.astimezone(ZoneInfo(tz_name))
    return local_dt.strftime("%Y%m%dT%H%M%S%z")



def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)



def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    ensure_parent_dir(path)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)



def parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    if normalized in {"1", "true", "t", "yes", "y"}:
        return True
    if normalized in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"Cannot parse boolean value: {value!r}")
