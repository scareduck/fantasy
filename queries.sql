-- Available starting pitchers with ESPN forecaster start projections
--
-- Requires:
--   - A completed yahoo_sync.py run (populates player_availability_snapshot)
--   - A completed espn_forecaster_sync.py run (populates espn_forecaster_snapshot)
--
-- Note: the current ESPN forecaster format provides start matchups (opponent,
-- home/away indicator embedded in matchup_text) but not FPTS projections.
-- start_1 = first projected start this week  (e.g. "Tue 4/14-@SD (King)")
-- start_2 = second projected start this week (e.g. "Sun 4/19-TEX (Gore)")

SELECT
    p.full_name,
    p.editorial_team_abbr          AS team,
    pas.availability_status,
    pas.percent_owned,
    efs.forecaster_for_date        AS week,
    efs.matchup_text               AS start_1,
    efs.projection_text            AS start_2
FROM player_availability_snapshot pas
JOIN player p
    ON p.player_id = pas.player_id
JOIN (
    SELECT
        player_id,
        matchup_text,
        projection_text,
        forecaster_for_date,
        ROW_NUMBER() OVER (
            PARTITION BY player_id
            ORDER BY captured_at_utc DESC
        ) AS rn
    FROM espn_forecaster_snapshot
    WHERE player_id IS NOT NULL
) efs
    ON efs.player_id = pas.player_id
    AND efs.rn = 1
WHERE pas.sync_run_id = (SELECT MAX(sync_run_id) FROM sync_run)
ORDER BY pas.availability_status, pas.percent_owned DESC\G
