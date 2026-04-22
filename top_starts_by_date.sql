-- Available starting pitchers ordered by game date, then FPTS descending.
-- Filters to starts projected >= 9 FPTS.
--
-- Requires:
--   - A completed yahoo_sync.py run (populates player_availability_snapshot)
--   - A completed espn_forecaster_sync.py run (populates espn_forecaster_snapshot)
--   - A completed pitcher_stats_sync.py run (populates pitcher_season_stats)

SELECT
    p.full_name,
    p.editorial_team_abbr                     AS team,
    efs.matchup_text                           AS start,
    STR_TO_DATE(
        CONCAT(YEAR(CURDATE()), '/',
               SUBSTRING_INDEX(SUBSTRING_INDEX(efs.matchup_text, '-', 1), ' ', -1)),
        '%Y/%m/%d'
    )                                          AS game_date,
    CAST(efs.projection_text AS DECIMAL(6,2)) AS fpts,
    CAST(cps.era AS DECIMAL(6,2))             AS era
FROM current_availability pas
JOIN player p
    ON p.player_id = pas.player_id
JOIN current_espn_forecast efs
    ON efs.player_id = pas.player_id
INNER JOIN current_pitcher_stats cps
    ON p.player_id = cps.player_id
WHERE pas.availability_status = 'fa'
  AND efs.projection_text >= 9
having game_date>=now()
ORDER BY game_date, fpts DESC;
