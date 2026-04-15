-- All teams' pitchers with individual start dates and FPTS per start.
-- Run after yahoo_sync.py --all-rosters and espn_forecaster_sync.py.

SELECT
    cr.team_name,
    STR_TO_DATE(CONCAT(SUBSTRING_INDEX(efs.matchup_text, '-', 1), ' 2026'), '%a %c/%e %Y') AS start_date,
    p.full_name,
    p.editorial_team_abbr AS team,
    cr.selected_position AS slot,
    efs.matchup_text AS start,
    CAST(efs.projection_text AS DECIMAL(6,2)) AS fpts
FROM current_roster cr
JOIN player p ON p.player_id = cr.player_id
JOIN current_espn_forecast efs ON efs.player_id = cr.player_id
WHERE efs.projection_text IS NOT NULL
ORDER BY cr.team_name, start_date, fpts DESC;
