-- Available starting pitchers with ESPN forecaster FPTS projections
--
-- Requires:
--   - A completed yahoo_sync.py run (populates player_availability_snapshot)
--   - A completed espn_forecaster_sync.py run (populates espn_forecaster_snapshot)
--
-- One row per pitcher. total_fpts is the sum of projected FPTS across all
-- starts in the current forecaster window. starts lists each matchup.

SELECT
    p.full_name,
    p.editorial_team_abbr                   AS team,
#    pas.availability_status as avail,
#    pas.percent_owned,
    efs.forecaster_for_date                 AS week,
    CAST(efs.projection_text AS DECIMAL(6,2)) AS fpts,
    efs.matchup_text AS starts
FROM current_availability pas
JOIN player p
    ON p.player_id = pas.player_id
JOIN current_espn_forecast efs
    ON efs.player_id = pas.player_id
WHERE pas.availability_status = 'fa'
ORDER BY fpts DESC
LIMIT 10;
