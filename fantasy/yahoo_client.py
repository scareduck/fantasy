from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import requests

from fantasy.config import Settings
from fantasy.yahoo_auth import YahooAuth
from fantasy.yahoo_xml import parse_game, parse_leagues, parse_league_settings, parse_players, parse_xml

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

    def get_xml(self, path: str) -> YahooResponse:
        token = self.auth.get_valid_token()
        url = f"{BASE_URL}/{path.lstrip('/')}"
        response = self.session.get(
            url,
            headers={
                "Authorization": f"Bearer {token.access_token}",
                "Accept": "application/xml",
            },
            timeout=60,
        )
        response.raise_for_status()
        return YahooResponse(url=url, body=response.text)

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
