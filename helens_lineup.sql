-- Helen's roster batters vs. their opposing starter's FPTS projection for today.
--
-- Requires:
--   - A completed yahoo_sync.py --all-rosters run (current_roster)
--   - A completed espn_forecaster_sync.py run (current_espn_forecast)

SELECT
    p.full_name                                     AS batter,
    p.editorial_team_abbr                           AS batter_team,
    cr.selected_position                            AS slot,
    opp.pitcher_name                                AS opp_pitcher,
    opp.team_abbr                                   AS pitcher_team,
    opp.matchup_text                                AS matchup,
    CAST(opp.projection_text AS DECIMAL(6,2))       AS pitcher_fpts
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
WHERE p.position_type = 'B'
  AND cr.selected_position != 'IL'
  AND cr.team_name = 'Tinker Evers'' Chance'
ORDER BY pitcher_fpts DESC;
