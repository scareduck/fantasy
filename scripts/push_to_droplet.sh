#!/bin/bash
# Push the local fantasy DB to the read-only web mirror on the droplet.
# Runs after fantasy-run-all / fantasy-espn so the site stays current.
set -euo pipefail

REMOTE=rlm@escr2.scareduck.com

mariadb-dump --single-transaction --no-tablespaces fantasy \
  | ssh "$REMOTE" "mariadb fantasy"

# Pull latest web files in case UI changed.
ssh "$REMOTE" "cd /var/www/fantasy && git pull --ff-only --quiet"

# The dump restores views with the home machine's stored collation_connection,
# which may conflict on the Droplet. Re-create with explicit COLLATE on every
# string column so the view is immune to session/server collation settings.
ssh "$REMOTE" "mariadb fantasy -e \"
CREATE OR REPLACE VIEW mlb_batter_matchup AS
  SELECT game_date,
    CONVERT(away_team_abbr   USING utf8mb4) COLLATE utf8mb4_unicode_ci AS batter_team,
    CONVERT(home_team_abbr   USING utf8mb4) COLLATE utf8mb4_unicode_ci AS pitcher_team,
    0 AS is_home,
    CONVERT(home_pitcher_name USING utf8mb4) COLLATE utf8mb4_unicode_ci AS opp_pitcher_name,
    home_pitcher_mlb_id AS opp_pitcher_mlb_id,
    home_pitcher_player_id AS opp_pitcher_player_id,
    CONVERT(CONCAT(DATE_FORMAT(game_date,'%a %c/%e'),'-@',home_team_abbr) USING utf8mb4) COLLATE utf8mb4_unicode_ci AS matchup_text
  FROM mlb_schedule
UNION ALL
  SELECT game_date,
    CONVERT(home_team_abbr   USING utf8mb4) COLLATE utf8mb4_unicode_ci AS batter_team,
    CONVERT(away_team_abbr   USING utf8mb4) COLLATE utf8mb4_unicode_ci AS pitcher_team,
    1 AS is_home,
    CONVERT(away_pitcher_name USING utf8mb4) COLLATE utf8mb4_unicode_ci AS opp_pitcher_name,
    away_pitcher_mlb_id AS opp_pitcher_mlb_id,
    away_pitcher_player_id AS opp_pitcher_player_id,
    CONVERT(CONCAT(DATE_FORMAT(game_date,'%a %c/%e'),'-',away_team_abbr) USING utf8mb4) COLLATE utf8mb4_unicode_ci AS matchup_text
  FROM mlb_schedule;
\""
