from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

TEAM_ABBR_MAP = {
    "WSH": "WSN",
    "WAS": "WSN",
    "NYY": "NYY",
    "NYM": "NYM",
    "CHW": "CWS",
    "CWS": "CWS",
    "KCR": "KC",
    "KAN": "KC",
    "SDP": "SD",
    "SFG": "SF",
    "TBR": "TB",
    "LAD": "LAD",
    "LAA": "LAA",
    "ARI": "ARI",
    "AZ": "ARI",
    "ATH": "OAK",
}

SUFFIX_RE = re.compile(r"\b(jr|sr|ii|iii|iv)\.?$", re.IGNORECASE)


@dataclass(frozen=True)
class PlayerMatchResult:
    player_id: int | None
    method: str
    confidence: str


def normalize_player_name(name: str | None) -> str:
    if not name:
        return ""
    normalized = re.sub(r"\s+", " ", name).strip().lower()
    normalized = re.sub(r"[,'’.-]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = SUFFIX_RE.sub("", normalized).strip()
    return normalized


def normalize_ascii_name(name: str | None) -> str:
    if not name:
        return ""
    ascii_name = unicodedata.normalize("NFKD", name)
    ascii_name = ascii_name.encode("ascii", "ignore").decode("ascii")
    return normalize_player_name(ascii_name)


def normalize_team_abbr(team_abbr: str | None) -> str:
    if not team_abbr:
        return ""
    cleaned = re.sub(r"[^A-Za-z]", "", team_abbr).upper()
    return TEAM_ABBR_MAP.get(cleaned, cleaned)


def correlate_forecaster_row(
    row: dict,
    explicit_map: dict[tuple[str, str], int],
    full_name_team_map: dict[tuple[str, str], list[int]],
    ascii_name_team_map: dict[tuple[str, str], list[int]],
) -> PlayerMatchResult:
    source_name = row.get("source_name", "espn_forecaster")
    external_id = str(row.get("espn_player_id") or "").strip()
    team_abbr = normalize_team_abbr(row.get("team_abbr"))
    norm_name = normalize_player_name(row.get("pitcher_name"))
    ascii_name = normalize_ascii_name(row.get("pitcher_name"))

    if external_id:
        mapped = explicit_map.get((source_name, external_id))
        if mapped:
            return PlayerMatchResult(player_id=mapped, method="player_external_id", confidence="high")

    if norm_name and team_abbr:
        by_full = full_name_team_map.get((norm_name, team_abbr), [])
        if len(by_full) == 1:
            return PlayerMatchResult(player_id=by_full[0], method="full_name_team", confidence="medium")
        if len(by_full) > 1:
            return PlayerMatchResult(player_id=None, method="ambiguous_full_name_team", confidence="low")

    if ascii_name and team_abbr:
        by_ascii = ascii_name_team_map.get((ascii_name, team_abbr), [])
        if len(by_ascii) == 1:
            return PlayerMatchResult(player_id=by_ascii[0], method="ascii_name_team", confidence="medium")
        if len(by_ascii) > 1:
            return PlayerMatchResult(player_id=None, method="ambiguous_ascii_name_team", confidence="low")

    return PlayerMatchResult(player_id=None, method="unresolved", confidence="none")
