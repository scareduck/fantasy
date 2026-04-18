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
    efs.total_fpts,
    efs.starts,
    cast(cps.era as decimal(6,2)) as era
FROM current_availability pas
JOIN player p
    ON p.player_id = pas.player_id
JOIN (
    SELECT
        player_id,
        forecaster_for_date,
        ROUND(SUM(CAST(projection_text AS DECIMAL(6,2))), 1) AS total_fpts,
        GROUP_CONCAT(matchup_text ORDER BY matchup_text SEPARATOR ', ') AS starts
    FROM current_espn_forecast
    WHERE player_id IS NOT NULL
    GROUP BY player_id, forecaster_for_date
) efs
    ON efs.player_id = pas.player_id
inner join current_pitcher_stats cps
    on p.player_id=cps.player_id
WHERE pas.availability_status='fa'
ORDER BY pas.availability_status, efs.total_fpts DESC
limit 10;
