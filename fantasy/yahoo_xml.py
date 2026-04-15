from __future__ import annotations

import json
from typing import Iterable
from xml.etree import ElementTree as ET

NS = {"y": "http://fantasysports.yahooapis.com/fantasy/v2/base.rng"}



def parse_xml(xml_text: str) -> ET.Element:
    return ET.fromstring(xml_text)



def find_text(node: ET.Element, path: str) -> str | None:
    found = node.find(path, NS)
    if found is None or found.text is None:
        return None
    value = found.text.strip()
    return value if value else None



def find_all(node: ET.Element, path: str) -> list[ET.Element]:
    return node.findall(path, NS)



def element_to_xml(node: ET.Element) -> str:
    return ET.tostring(node, encoding="unicode")



def parse_game(root: ET.Element) -> dict:
    game = root.find("y:game", NS)
    if game is None:
        raise RuntimeError("Yahoo response did not contain a game resource")
    return {
        "game_key": find_text(game, "y:game_key"),
        "game_id": find_text(game, "y:game_id"),
        "code": find_text(game, "y:code"),
        "name": find_text(game, "y:name"),
        "season": int(find_text(game, "y:season") or 0) or None,
        "url": find_text(game, "y:url"),
    }



def parse_leagues(root: ET.Element) -> list[dict]:
    leagues: list[dict] = []
    for league in root.findall(".//y:league", NS):
        league_key = find_text(league, "y:league_key")
        if not league_key:
            continue
        leagues.append(
            {
                "league_key": league_key,
                "league_id": find_text(league, "y:league_id"),
                "name": find_text(league, "y:name"),
                "url": find_text(league, "y:url"),
                "draft_status": find_text(league, "y:draft_status"),
                "num_teams": int(find_text(league, "y:num_teams") or 0) or None,
                "scoring_type": find_text(league, "y:scoring_type"),
                "season": int(find_text(league, "y:season") or 0) or None,
            }
        )
    seen: set[str] = set()
    unique: list[dict] = []
    for item in leagues:
        if item["league_key"] in seen:
            continue
        seen.add(item["league_key"])
        unique.append(item)
    return unique



def parse_league_settings(root: ET.Element) -> dict:
    league = root.find("y:league", NS)
    if league is None:
        raise RuntimeError("Yahoo response did not contain a league resource")

    categories: list[dict] = []
    for stat in league.findall(".//y:settings/y:stat_categories/y:stats/y:stat", NS):
        categories.append(
            {
                "stat_id": int(find_text(stat, "y:stat_id") or 0),
                "enabled": int(find_text(stat, "y:enabled") or 0),
                "name": find_text(stat, "y:name"),
                "display_name": find_text(stat, "y:display_name"),
                "sort_order": int(find_text(stat, "y:sort_order") or 0),
                "position_type": find_text(stat, "y:position_type"),
            }
        )

    return {
        "league_key": find_text(league, "y:league_key"),
        "name": find_text(league, "y:name"),
        "url": find_text(league, "y:url"),
        "scoring_type": find_text(league, "y:scoring_type"),
        "num_teams": int(find_text(league, "y:num_teams") or 0) or None,
        "categories": categories,
        "raw_xml": ET.tostring(root, encoding="unicode"),
    }



def parse_players(root: ET.Element) -> list[dict]:
    players: list[dict] = []
    for player in root.findall(".//y:player", NS):
        player_key = find_text(player, "y:player_key")
        if not player_key:
            continue
        eligible_positions = [
            pos.text.strip()
            for pos in player.findall("y:eligible_positions/y:position", NS)
            if pos.text and pos.text.strip()
        ]
        players.append(
            {
                "yahoo_player_key": player_key,
                "yahoo_player_id": int(find_text(player, "y:player_id") or 0) or None,
                "editorial_player_key": find_text(player, "y:editorial_player_key"),
                "full_name": find_text(player, "y:name/y:full") or "UNKNOWN",
                "first_name": find_text(player, "y:name/y:first"),
                "last_name": find_text(player, "y:name/y:last"),
                "ascii_first_name": find_text(player, "y:name/y:ascii_first"),
                "ascii_last_name": find_text(player, "y:name/y:ascii_last"),
                "editorial_team_key": find_text(player, "y:editorial_team_key"),
                "editorial_team_full_name": find_text(player, "y:editorial_team_full_name"),
                "editorial_team_abbr": find_text(player, "y:editorial_team_abbr"),
                "uniform_number": find_text(player, "y:uniform_number"),
                "display_position": find_text(player, "y:display_position"),
                "position_type": find_text(player, "y:position_type"),
                "eligible_positions": eligible_positions,
                "eligible_positions_json": json.dumps(eligible_positions),
                "yahoo_status": find_text(player, "y:status"),
                "yahoo_status_full": find_text(player, "y:status_full"),
                "percent_owned": _parse_percent_owned(player),
                "raw_player_xml": element_to_xml(player),
            }
        )
    return players



def parse_player_stats(root: ET.Element) -> dict[str, dict[int, str | None]]:
    """Parse player season stats from a players;.../stats response.

    Returns a mapping of yahoo_player_key -> {stat_id: raw_value_string}.
    Values are kept as raw strings so callers can handle composite stats like
    H/AB ("45/167") without losing information.
    """
    result: dict[str, dict[int, str | None]] = {}
    for player in root.findall(".//y:player", NS):
        player_key = find_text(player, "y:player_key")
        if not player_key:
            continue
        stats: dict[int, str | None] = {}
        for stat in player.findall(".//y:player_stats/y:stats/y:stat", NS):
            stat_id_text = find_text(stat, "y:stat_id")
            value_text = find_text(stat, "y:value")
            if stat_id_text is None:
                continue
            stat_id = int(stat_id_text)
            stats[stat_id] = None if (value_text is None or value_text == "-") else value_text
        result[player_key] = stats
    return result


def _parse_percent_owned(player: ET.Element) -> float | None:
    value = find_text(player, "y:percent_owned/y:value")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_teams(root: ET.Element) -> list[dict]:
    """Parse teams from a league/teams response. Returns list of team dicts."""
    teams: list[dict] = []
    for team in root.findall(".//y:team", NS):
        team_key = find_text(team, "y:team_key")
        if not team_key:
            continue
        teams.append(
            {
                "team_key": team_key,
                "team_id": find_text(team, "y:team_id"),
                "team_name": find_text(team, "y:name"),
                "is_owned_by_current_login": find_text(team, "y:is_owned_by_current_login") == "1",
            }
        )
    return teams


def parse_roster_players(root: ET.Element) -> list[dict]:
    """Parse players from a team/roster response, including selected_position."""
    players: list[dict] = []
    for player in root.findall(".//y:player", NS):
        player_key = find_text(player, "y:player_key")
        if not player_key:
            continue
        eligible_positions = [
            pos.text.strip()
            for pos in player.findall("y:eligible_positions/y:position", NS)
            if pos.text and pos.text.strip()
        ]
        selected_position = find_text(player, "y:selected_position/y:position")
        players.append(
            {
                "yahoo_player_key": player_key,
                "yahoo_player_id": int(find_text(player, "y:player_id") or 0) or None,
                "editorial_player_key": find_text(player, "y:editorial_player_key"),
                "full_name": find_text(player, "y:name/y:full") or "UNKNOWN",
                "first_name": find_text(player, "y:name/y:first"),
                "last_name": find_text(player, "y:name/y:last"),
                "ascii_first_name": find_text(player, "y:name/y:ascii_first"),
                "ascii_last_name": find_text(player, "y:name/y:ascii_last"),
                "editorial_team_key": find_text(player, "y:editorial_team_key"),
                "editorial_team_full_name": find_text(player, "y:editorial_team_full_name"),
                "editorial_team_abbr": find_text(player, "y:editorial_team_abbr"),
                "uniform_number": find_text(player, "y:uniform_number"),
                "display_position": find_text(player, "y:display_position"),
                "position_type": find_text(player, "y:position_type"),
                "eligible_positions": eligible_positions,
                "eligible_positions_json": json.dumps(eligible_positions),
                "yahoo_status": find_text(player, "y:status"),
                "yahoo_status_full": find_text(player, "y:status_full"),
                "percent_owned": _parse_percent_owned(player),
                "raw_player_xml": element_to_xml(player),
                "selected_position": selected_position,
            }
        )
    return players
