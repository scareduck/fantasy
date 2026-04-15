-- Helen's roster batters vs. their opposing starter's FPTS projection and season ERA.
--
-- Requires:
--   - A completed yahoo_sync.py --all-rosters run (current_roster)
--   - A completed espn_forecaster_sync.py run (current_espn_forecast)
--   - A completed fantasy-pitcher-stats run (current_pitcher_stats)

SELECT
    p.full_name                                     AS batter,
    p.editorial_team_abbr                           AS batter_team,
    cr.selected_position                            AS slot,
    opp.pitcher_name                                AS opp_pitcher,
    opp.team_abbr                                   AS pitcher_team,
    opp.matchup_text                                AS matchup,
    CAST(opp.projection_text AS DECIMAL(6,2))       AS FPTS,
    cps.era                                         AS ERA
FROM current_roster cr
JOIN player p ON p.player_id = cr.player_id
LEFT JOIN current_espn_forecast opp
    ON opp.opponent_team_abbr = p.editorial_team_abbr
    AND opp.matchup_text LIKE CONCAT(
            '%',
            MONTH(CURDATE()), '/', DAY(CURDATE()),
            '%'
        )
    AND opp.projection_text IS NOT NULL
LEFT JOIN current_pitcher_stats cps ON cps.player_id = opp.player_id
WHERE p.position_type = 'B'
  AND cr.selected_position != 'IL'
  AND cr.team_name LIKE 'Tinker Evers%'
ORDER BY FPTS DESC;
