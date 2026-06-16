#!/bin/bash
# Push the local fantasy DB to the read-only web mirror on the droplet.
# Runs after fantasy-run-all / fantasy-espn so the site stays current.
set -euo pipefail

REMOTE=rlm@escr2.scareduck.com

mariadb-dump --single-transaction --no-tablespaces fantasy \
  | ssh "$REMOTE" "mariadb fantasy"

# Pull latest web files in case UI changed.
ssh "$REMOTE" "cd /var/www/fantasy && git pull --ff-only --quiet"

# The dump preserves views with the home machine's collation_connection
# (utf8mb4_uca1400_ai_ci), which conflicts with utf8mb4_general_ci used by
# PHP's mysqli. Re-create the affected view with utf8mb4_unicode_ci so the
# UNION over mlb_schedule columns doesn't throw a collation mismatch.
ssh "$REMOTE" "mariadb fantasy -e \"
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE OR REPLACE VIEW mlb_batter_matchup AS
  SELECT game_date, away_team_abbr AS batter_team, home_team_abbr AS pitcher_team,
         0 AS is_home, home_pitcher_name AS opp_pitcher_name,
         home_pitcher_mlb_id AS opp_pitcher_mlb_id,
         home_pitcher_player_id AS opp_pitcher_player_id,
         CONCAT(DATE_FORMAT(game_date,'%a %c/%e'),'-@',home_team_abbr) AS matchup_text
  FROM mlb_schedule
UNION ALL
  SELECT game_date, home_team_abbr AS batter_team, away_team_abbr AS pitcher_team,
         1 AS is_home, away_pitcher_name AS opp_pitcher_name,
         away_pitcher_mlb_id AS opp_pitcher_mlb_id,
         away_pitcher_player_id AS opp_pitcher_player_id,
         CONCAT(DATE_FORMAT(game_date,'%a %c/%e'),'-',away_team_abbr) AS matchup_text
  FROM mlb_schedule;
\""
