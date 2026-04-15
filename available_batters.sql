-- Free-agent batters ranked by OBP, filtered by playing time.
--
-- Requires a completed fantasy-batter-sync run.
--
-- Adjust the HAVING ab >= N threshold to taste:
--   Early season  (~2 weeks in): 20
--   Mid-season    (~6 weeks in): 50
--   Late season   (Aug+):        80

SELECT
    p.full_name                                                    AS name,
    p.editorial_team_abbr                                          AS team,
    p.display_position                                             AS pos,
    pas.percent_owned                                              AS pct_own,
    bss.ab,
    bss.obp,
    bss.r,
    bss.hr,
    bss.rbi,
    bss.sb
FROM player_availability_snapshot pas
JOIN player p
    ON p.player_id = pas.player_id
JOIN batter_season_stats bss
    ON bss.player_id = pas.player_id
    AND bss.sync_run_id = pas.sync_run_id
WHERE pas.sync_run_id = (
        SELECT MAX(sync_run_id)
        FROM sync_run
        WHERE requested_position = 'B'
    )
  AND pas.availability_status = 'FA'
HAVING ab >= 50
ORDER BY obp DESC, ab DESC
LIMIT 30;
