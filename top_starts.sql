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
    pas.availability_status as avail,
#    pas.percent_owned,
    efs.forecaster_for_date                 AS week,
    CAST(efs.projection_text AS DECIMAL(6,2)) as fpts,
    efs.matchup_text as starts
FROM player_availability_snapshot pas
JOIN player p
    ON p.player_id = pas.player_id
JOIN espn_forecaster_snapshot efs
    ON efs.player_id = pas.player_id
WHERE pas.sync_run_id = (SELECT MAX(sync_run_id) FROM sync_run)
  and pas.availability_status='fa'
  and efs.captured_at_utc=(select max(captured_at_utc) from espn_forecaster_snapshot)
ORDER BY fpts DESC
limit 10;
