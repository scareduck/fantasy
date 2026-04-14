-- All teams' pitchers with individual start dates and FPTS per start.
-- Run after yahoo_sync.py --all-rosters and espn_forecaster_sync.py.

SELECT
    rs.team_name,
    STR_TO_DATE(CONCAT(SUBSTRING_INDEX(efs.matchup_text, '-', 1), ' 2026'), '%a %c/%e %Y') AS start_date,
    p.full_name,
    p.editorial_team_abbr AS team,
    rs.selected_position AS slot,
    efs.matchup_text AS start,
    CAST(efs.projection_text AS DECIMAL(6,2)) AS fpts
FROM roster_snapshot rs
JOIN player p ON p.player_id = rs.player_id
JOIN espn_forecaster_snapshot efs ON efs.player_id = rs.player_id
WHERE rs.captured_at_utc = (SELECT MAX(captured_at_utc) FROM roster_snapshot)
  AND efs.captured_at_utc = (SELECT MAX(captured_at_utc) FROM espn_forecaster_snapshot)
  AND efs.projection_text IS NOT NULL
ORDER BY rs.team_name, start_date, fpts DESC;
