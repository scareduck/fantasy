from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable

import requests

from fantasy.config import Settings
from fantasy.yahoo_auth import YahooAuth
from fantasy.yahoo_xml import parse_game, parse_leagues, parse_league_settings, parse_player_stats, parse_players, parse_roster_players, parse_teams, parse_xml

BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"


@dataclass
class YahooResponse:
    url: str
    body: str


class YahooFantasyClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self.auth = YahooAuth(settings, self.session)

    def get_xml(self, path: str, *, retries: int = 5, backoff: float = 20.0) -> YahooResponse:
        url = f"{BASE_URL}/{path.lstrip('/')}"
        last_err: Exception = RuntimeError("No attempts made")
        for attempt in range(retries):
            if attempt:
                delay = backoff * attempt
                print(f"  [retry {attempt}/{retries-1}] waiting {delay:.0f}s: {url}")
                time.sleep(delay)
            try:
                token = self.auth.get_valid_token()
                response = self.session.get(
                    url,
                    headers={
                        "Authorization": f"Bearer {token.access_token}",
                        "Accept": "application/xml",
                    },
                    timeout=60,
                )
                response.raise_for_status()
                body = response.text
                if body.strip().startswith("<"):
                    return YahooResponse(url=url, body=body)
                last_err = RuntimeError(f"Non-XML response: {body[:120]!r}")
                print(f"  [attempt {attempt+1}] non-XML response from Yahoo, will retry")
            except requests.HTTPError as exc:
                last_err = exc
                print(f"  [attempt {attempt+1}] HTTP {exc.response.status_code}, will retry")
            except requests.RequestException as exc:
                last_err = exc
                print(f"  [attempt {attempt+1}] request error: {exc}, will retry")
        raise last_err

    def get_current_mlb_game(self) -> dict:
        xml = self.get_xml("game/mlb")
        return parse_game(parse_xml(xml.body))

    def get_user_leagues_for_game(self, game_key: str) -> list[dict]:
        xml = self.get_xml(f"users;use_login=1/games;game_keys={game_key}/leagues")
        return parse_leagues(parse_xml(xml.body))

    def get_league_settings(self, league_key: str) -> dict:
        xml = self.get_xml(f"league/{league_key}/settings")
        return parse_league_settings(parse_xml(xml.body))

    def get_league_players_page(
        self,
        league_key: str,
        *,
        status: str,
        position: str,
        start: int,
        count: int,
    ) -> list[dict]:
        xml = self.get_xml(
            f"league/{league_key}/players;status={status};position={position};start={start};count={count}"
        )
        return parse_players(parse_xml(xml.body))

    def get_league_players_stats_page(
        self,
        league_key: str,
        *,
        status: str,
        position: str,
        start: int,
        count: int,
    ) -> dict[str, dict[int, str | None]]:
        """Fetch the same player page but with the /stats sub-resource.

        Returns a mapping of yahoo_player_key -> {stat_id: value}.
        """
        xml = self.get_xml(
            f"league/{league_key}/players;status={status};position={position};start={start};count={count}/stats"
        )
        return parse_player_stats(parse_xml(xml.body))

    def get_league_players_stats_date_page(
        self,
        league_key: str,
        *,
        game_date: str,
        status: str,
        position: str,
        start: int,
        count: int,
    ) -> dict[str, dict[int, str | None]]:
        """Fetch per-date stats for a page of players.

        Returns a mapping of yahoo_player_key -> {stat_id: value}.
        """
        xml = self.get_xml(
            f"league/{league_key}/players;status={status};position={position};start={start};count={count}/stats;type=date;date={game_date}"
        )
        return parse_player_stats(parse_xml(xml.body))

    def get_league_teams(self, league_key: str) -> list[dict]:
        xml = self.get_xml(f"league/{league_key}/teams")
        return parse_teams(parse_xml(xml.body))

    def get_team_roster(self, team_key: str) -> list[dict]:
        xml = self.get_xml(f"team/{team_key}/roster/players")
        return parse_roster_players(parse_xml(xml.body))
