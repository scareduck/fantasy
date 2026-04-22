-- Free-agent pitchers ranked by ERA, filtered by playing time and health.
--
-- Requires a completed yahoo_sync.py and pitcher-stats sync run.
--
-- Adjust the HAVING ip >= N threshold to taste:
--   Early season  (~2 weeks in): 10
--   Mid-season    (~6 weeks in): 25
--   Late season   (Aug+):        50

SELECT
    p.full_name                                                     AS name,
    p.editorial_team_abbr                                           AS team,
    p.display_position                                              AS pos,
    p.yahoo_status                                                  AS status,
    ca.percent_owned                                                AS pct_own,
    cps.ip,
    cps.w,
    cps.k,
    CAST(cps.era  AS DECIMAL(5,2))                                  AS era,
    CAST(cps.whip AS DECIMAL(5,3))                                  AS whip
FROM current_availability ca
JOIN player p
    ON p.player_id = ca.player_id
JOIN current_pitcher_stats cps
    ON cps.player_id = ca.player_id
WHERE ca.availability_status = 'fa'
  AND (p.yahoo_status IS NULL OR p.yahoo_status NOT IN ('NA', 'IL'))
HAVING ip >= 10
ORDER BY era ASC, ip DESC
LIMIT 30;
