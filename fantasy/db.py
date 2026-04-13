from __future__ import annotations

import json

import mariadb

from fantasy.config import Settings



def connect(settings: Settings) -> mariadb.Connection:
    conn = mariadb.connect(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        database=settings.db_name,
        autocommit=False,
    )
    return conn



def get_league_by_key(conn: mariadb.Connection, league_key: str) -> dict | None:
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT league_id, yahoo_league_key, league_name FROM league WHERE yahoo_league_key = ?",
        (league_key,),
    )
    return cur.fetchone()



def upsert_league(conn: mariadb.Connection, league_payload: dict, game_payload: dict, settings_payload: dict) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO league (
            yahoo_league_key,
            yahoo_game_key,
            game_code,
            season,
            league_name,
            scoring_type,
            num_teams,
            league_url,
            last_synced_at_utc,
            raw_settings_xml
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(), ?)
        ON DUPLICATE KEY UPDATE
            yahoo_game_key = VALUES(yahoo_game_key),
            game_code = VALUES(game_code),
            season = VALUES(season),
            league_name = VALUES(league_name),
            scoring_type = VALUES(scoring_type),
            num_teams = VALUES(num_teams),
            league_url = VALUES(league_url),
            last_synced_at_utc = UTC_TIMESTAMP(),
            raw_settings_xml = VALUES(raw_settings_xml)
        """,
        (
            league_payload["league_key"],
            game_payload.get("game_key"),
            game_payload.get("code"),
            game_payload.get("season"),
            settings_payload.get("name") or league_payload.get("name"),
            settings_payload.get("scoring_type") or league_payload.get("scoring_type"),
            settings_payload.get("num_teams") or league_payload.get("num_teams"),
            settings_payload.get("url") or league_payload.get("url"),
            settings_payload.get("raw_xml"),
        ),
    )

    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT league_id FROM league WHERE yahoo_league_key = ?",
        (league_payload["league_key"],),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("Failed to resolve league_id after upsert")
    return int(row["league_id"])



def upsert_league_stat_categories(conn: mariadb.Connection, league_id: int, categories: list[dict]) -> None:
    cur = conn.cursor()
    for category in categories:
        cur.execute(
            """
            INSERT INTO league_stat_category (
                league_id,
                stat_id,
                stat_name,
                display_name,
                position_type,
                sort_order,
                is_enabled,
                is_focus_category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                stat_name = VALUES(stat_name),
                display_name = VALUES(display_name),
                position_type = VALUES(position_type),
                sort_order = VALUES(sort_order),
                is_enabled = VALUES(is_enabled)
            """,
            (
                league_id,
                category["stat_id"],
                category.get("name"),
                category.get("display_name"),
                category.get("position_type"),
                category.get("sort_order"),
                category.get("enabled", 1),
                1 if (category.get("display_name") or "") in {"R", "HR", "RBI", "SB", "OBP", "W", "K", "ERA", "WHIP", "SV+H"} else 0,
            ),
        )



def create_sync_run(
    conn: mariadb.Connection,
    *,
    league_id: int,
    requested_position: str,
    requested_statuses: str,
    snapshot_file: str,
    notes: str | None = None,
) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO sync_run (
            league_id,
            requested_position,
            requested_statuses,
            snapshot_file,
            notes
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (league_id, requested_position, requested_statuses, snapshot_file, notes),
    )
    return int(cur.lastrowid)



def complete_sync_run(conn: mariadb.Connection, sync_run_id: int, row_count: int) -> None:
    cur = conn.cursor()
    cur.execute(
        "UPDATE sync_run SET completed_at_utc = UTC_TIMESTAMP(), row_count = ? WHERE sync_run_id = ?",
        (row_count, sync_run_id),
    )



def upsert_player(conn: mariadb.Connection, player_payload: dict) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO player (
            yahoo_player_key,
            yahoo_player_id,
            editorial_player_key,
            full_name,
            first_name,
            last_name,
            ascii_first_name,
            ascii_last_name,
            editorial_team_key,
            editorial_team_full_name,
            editorial_team_abbr,
            uniform_number,
            display_position,
            position_type,
            eligible_positions_json,
            yahoo_status,
            yahoo_status_full,
            raw_player_xml
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            yahoo_player_id = VALUES(yahoo_player_id),
            editorial_player_key = VALUES(editorial_player_key),
            full_name = VALUES(full_name),
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            ascii_first_name = VALUES(ascii_first_name),
            ascii_last_name = VALUES(ascii_last_name),
            editorial_team_key = VALUES(editorial_team_key),
            editorial_team_full_name = VALUES(editorial_team_full_name),
            editorial_team_abbr = VALUES(editorial_team_abbr),
            uniform_number = VALUES(uniform_number),
            display_position = VALUES(display_position),
            position_type = VALUES(position_type),
            eligible_positions_json = VALUES(eligible_positions_json),
            yahoo_status = VALUES(yahoo_status),
            yahoo_status_full = VALUES(yahoo_status_full),
            raw_player_xml = VALUES(raw_player_xml)
        """,
        (
            player_payload["yahoo_player_key"],
            player_payload.get("yahoo_player_id"),
            player_payload.get("editorial_player_key"),
            player_payload.get("full_name"),
            player_payload.get("first_name"),
            player_payload.get("last_name"),
            player_payload.get("ascii_first_name"),
            player_payload.get("ascii_last_name"),
            player_payload.get("editorial_team_key"),
            player_payload.get("editorial_team_full_name"),
            player_payload.get("editorial_team_abbr"),
            player_payload.get("uniform_number"),
            player_payload.get("display_position"),
            player_payload.get("position_type"),
            player_payload.get("eligible_positions_json"),
            player_payload.get("yahoo_status"),
            player_payload.get("yahoo_status_full"),
            player_payload.get("raw_player_xml"),
        ),
    )
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT player_id FROM player WHERE yahoo_player_key = ?",
        (player_payload["yahoo_player_key"],),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("Failed to resolve player_id after upsert")
    return int(row["player_id"])



def insert_availability_snapshot(
    conn: mariadb.Connection,
    *,
    sync_run_id: int,
    league_id: int,
    player_id: int,
    captured_at_utc,
    availability_status: str,
    source_page_start: int,
    source_page_count: int,
    percent_owned: float | None,
    raw_player_xml: str | None,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO player_availability_snapshot (
            sync_run_id,
            league_id,
            player_id,
            captured_at_utc,
            availability_status,
            source_page_start,
            source_page_count,
            percent_owned,
            raw_player_xml
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            captured_at_utc = VALUES(captured_at_utc),
            source_page_start = VALUES(source_page_start),
            source_page_count = VALUES(source_page_count),
            percent_owned = VALUES(percent_owned),
            raw_player_xml = VALUES(raw_player_xml)
        """,
        (
            sync_run_id,
            league_id,
            player_id,
            captured_at_utc,
            availability_status,
            source_page_start,
            source_page_count,
            percent_owned,
            raw_player_xml,
        ),
    )



def resolve_player_id(
    conn: mariadb.Connection,
    *,
    yahoo_player_key: str | None = None,
    player_name: str | None = None,
    editorial_team_abbr: str | None = None,
) -> int | None:
    cur = conn.cursor(dictionary=True)
    if yahoo_player_key:
        cur.execute(
            "SELECT player_id FROM player WHERE yahoo_player_key = ?",
            (yahoo_player_key,),
        )
        row = cur.fetchone()
        return int(row["player_id"]) if row else None

    if player_name and editorial_team_abbr:
        cur.execute(
            """
            SELECT player_id
            FROM player
            WHERE full_name = ? AND editorial_team_abbr = ?
            ORDER BY updated_at_utc DESC
            LIMIT 1
            """,
            (player_name, editorial_team_abbr),
        )
        row = cur.fetchone()
        return int(row["player_id"]) if row else None

    return None


def load_external_player_map(conn: mariadb.Connection, source_name: str) -> dict[tuple[str, str], int]:
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT source_name, external_id, player_id
        FROM player_external_id
        WHERE source_name = ?
        """,
        (source_name,),
    )
    mapping: dict[tuple[str, str], int] = {}
    for row in cur.fetchall():
        mapping[(row["source_name"], row["external_id"])] = int(row["player_id"])
    return mapping


def load_pitcher_name_team_maps(conn: mariadb.Connection) -> tuple[dict[tuple[str, str], list[int]], dict[tuple[str, str], list[int]]]:
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
            player_id,
            full_name,
            ascii_first_name,
            ascii_last_name,
            editorial_team_abbr,
            position_type,
            display_position
        FROM player
        WHERE
            (
                position_type = 'P'
                OR display_position LIKE '%SP%'
                OR display_position LIKE '%RP%'
                OR display_position = 'P'
            )
            AND editorial_team_abbr IS NOT NULL
            AND full_name IS NOT NULL
        """,
    )
    full_map: dict[tuple[str, str], list[int]] = {}
    ascii_map: dict[tuple[str, str], list[int]] = {}
    from fantasy.espn_forecaster import normalize_ascii_name, normalize_player_name, normalize_team_abbr

    for row in cur.fetchall():
        player_id = int(row["player_id"])
        team = normalize_team_abbr(row.get("editorial_team_abbr"))
        full_name = normalize_player_name(row.get("full_name"))
        if full_name and team:
            full_map.setdefault((full_name, team), []).append(player_id)

        ascii_name = normalize_ascii_name(row.get("full_name"))
        if not ascii_name:
            ascii_name = normalize_player_name(
                f"{row.get('ascii_first_name') or ''} {row.get('ascii_last_name') or ''}".strip()
            )
        if ascii_name and team:
            ascii_map.setdefault((ascii_name, team), []).append(player_id)

    return full_map, ascii_map


def insert_espn_forecaster_snapshot(
    conn: mariadb.Connection,
    *,
    source_name: str,
    captured_at_utc,
    forecaster_for_date,
    espn_player_id: str | None,
    pitcher_name: str,
    team_abbr: str | None,
    opponent_team_abbr: str | None,
    matchup_text: str | None,
    projection_text: str | None,
    player_id: int | None,
    match_method: str,
    raw_row_payload: dict,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO espn_forecaster_snapshot (
            source_name,
            captured_at_utc,
            forecaster_for_date,
            espn_player_id,
            pitcher_name,
            team_abbr,
            opponent_team_abbr,
            matchup_text,
            projection_text,
            player_id,
            match_method,
            raw_row_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_name,
            captured_at_utc,
            forecaster_for_date,
            espn_player_id,
            pitcher_name,
            team_abbr,
            opponent_team_abbr,
            matchup_text,
            projection_text,
            player_id,
            match_method,
            json.dumps(raw_row_payload, ensure_ascii=False),
        ),
    )
