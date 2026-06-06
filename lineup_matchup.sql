-- Today's batters vs. their opposing starter's FPTS projection and season ERA.
--
-- Requires:
--   - A completed yahoo_sync.py --all-rosters run (current_roster)
--   - A completed mlb_schedule_sync.py run (mlb_batter_matchup)
--   - A completed espn_forecaster_sync.py run (current_espn_forecast, for FPTS)
--   - A completed fantasy-pitcher-stats run (current_pitcher_stats)
--
-- off_day = 1 means no MLB game scheduled; opp_pitcher NULL with off_day = 0 means TBD starter.
-- Filter to a single team with: WHERE cr.team_name = 'Miskatonic Cthulhus'

SELECT
    cr.team_name,
    p.full_name                                         AS batter,
    p.editorial_team_abbr                               AS batter_team,
    cr.selected_position                                AS slot,
    COALESCE(opp.pitcher_name, bmatch.opp_pitcher_name) AS opp_pitcher,
    COALESCE(opp.team_abbr,    bmatch.pitcher_team)     AS pitcher_team,
    COALESCE(opp.matchup_text, bmatch.matchup_text)     AS matchup,
    CAST(opp.projection_text AS DECIMAL(6,2))           AS FPTS,
    cps.era                                             AS ERA,
    (bmatch.game_date IS NULL)                          AS off_day
FROM current_roster cr
JOIN player p ON p.player_id = cr.player_id
LEFT JOIN mlb_batter_matchup bmatch
    ON  bmatch.batter_team = p.editorial_team_abbr
    AND bmatch.game_date = CURDATE()
LEFT JOIN current_espn_forecast opp
    ON  opp.opponent_team_abbr = p.editorial_team_abbr
    AND opp.matchup_text LIKE CONCAT(
            '%',
            MONTH(CURDATE()), '/', DAY(CURDATE()),
            '%'
        )
    AND opp.projection_text IS NOT NULL
LEFT JOIN current_pitcher_stats cps
    ON cps.player_id = COALESCE(opp.player_id, bmatch.opp_pitcher_player_id)
WHERE p.position_type = 'B'
  AND cr.selected_position != 'IL'
ORDER BY cr.team_name, FPTS DESC;
