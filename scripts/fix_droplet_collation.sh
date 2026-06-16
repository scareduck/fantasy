#!/bin/bash
# Fix view collations after a nightly mariadb-dump restore from the home machine.
# The home machine uses utf8mb4_uca1400_ai_ci; the dump preserves that collation
# in view definitions. PHP's set_charset() locks prepared-statement parameters to
# the protocol-level collation, so views stored with uca1400 collide with the
# utf8mb4_unicode_ci table columns in LIKE/UNION operations.
# Run this after each nightly restore, e.g. via cron.
set -euo pipefail

mariadb -u rlm fantasy <<'SQL'
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE OR REPLACE VIEW mlb_batter_matchup AS
  SELECT game_date,
         away_team_abbr         AS batter_team,
         home_team_abbr         AS pitcher_team,
         0                      AS is_home,
         home_pitcher_name      AS opp_pitcher_name,
         home_pitcher_mlb_id    AS opp_pitcher_mlb_id,
         home_pitcher_player_id AS opp_pitcher_player_id,
         CONCAT(DATE_FORMAT(game_date, '%a %c/%e'), '-@', home_team_abbr) AS matchup_text
  FROM mlb_schedule
UNION ALL
  SELECT game_date,
         home_team_abbr         AS batter_team,
         away_team_abbr         AS pitcher_team,
         1                      AS is_home,
         away_pitcher_name      AS opp_pitcher_name,
         away_pitcher_mlb_id    AS opp_pitcher_mlb_id,
         away_pitcher_player_id AS opp_pitcher_player_id,
         CONCAT(DATE_FORMAT(game_date, '%a %c/%e'), '-', away_team_abbr) AS matchup_text
  FROM mlb_schedule;

CREATE OR REPLACE VIEW current_batter_stats AS
  SELECT bss.batter_season_stats_id, bss.sync_run_id, bss.player_id,
         bss.captured_at_utc, bss.ab, bss.r, bss.h, bss.hr, bss.rbi,
         bss.sb, bss.bb, bss.obp, bss.created_at_utc
  FROM batter_season_stats bss
  WHERE bss.sync_run_id = (SELECT MAX(sync_run_id) FROM batter_season_stats);

CREATE OR REPLACE VIEW current_roster AS
  SELECT rs.roster_snapshot_id, rs.league_id, rs.team_key, rs.team_name,
         rs.player_id, rs.yahoo_player_key, rs.selected_position, rs.captured_at_utc
  FROM roster_snapshot rs
  WHERE rs.captured_at_utc = (SELECT MAX(captured_at_utc) FROM roster_snapshot);

CREATE OR REPLACE VIEW current_espn_forecast AS
  SELECT efs.espn_forecaster_snapshot_id, efs.source_name, efs.captured_at_utc,
         efs.forecaster_for_date, efs.espn_player_id, efs.pitcher_name,
         efs.team_abbr, efs.opponent_team_abbr, efs.matchup_text,
         efs.projection_text, efs.player_id, efs.match_method,
         efs.raw_row_payload, efs.created_at_utc, efs.updated_at_utc
  FROM espn_forecaster_snapshot efs
  WHERE efs.captured_at_utc = (SELECT MAX(captured_at_utc) FROM espn_forecaster_snapshot);

CREATE OR REPLACE VIEW current_pitcher_stats AS
  SELECT pss.pitcher_season_stats_id, pss.sync_run_id, pss.player_id,
         pss.captured_at_utc, pss.ip, pss.w, pss.k, pss.era, pss.whip,
         pss.sv_holds, pss.created_at_utc
  FROM pitcher_season_stats pss
  WHERE pss.sync_run_id = (
    SELECT MAX(sync_run_id) FROM sync_run
    WHERE requested_position = 'P' AND requested_statuses = 'A,T'
  );

CREATE OR REPLACE VIEW current_availability AS
  SELECT pas.snapshot_id, pas.sync_run_id, pas.league_id, pas.player_id,
         pas.captured_at_utc, pas.availability_status, pas.source_page_start,
         pas.source_page_count, pas.percent_owned, pas.raw_player_xml,
         pas.created_at_utc
  FROM player_availability_snapshot pas
  WHERE pas.sync_run_id = (
    SELECT MAX(pas2.sync_run_id)
    FROM player_availability_snapshot pas2
    JOIN sync_run sr ON sr.sync_run_id = pas2.sync_run_id
    WHERE sr.requested_position = 'P'
  );

CREATE OR REPLACE VIEW current_rotowire_injuries AS
  SELECT rotowire_injury_snapshot_id, captured_at_utc, rotowire_player_id,
         player_name, team, position, injury, status, r_date, rotowire_url
  FROM rotowire_injury_snapshot
  WHERE captured_at_utc = (SELECT MAX(captured_at_utc) FROM rotowire_injury_snapshot);
SQL

echo "$(date): all view collations fixed."
