-- Players on the real MLB IL who are still rostered in the fantasy league.
--
-- Uses yahoo_status from the player table (sourced from ESPN/Yahoo sync),
-- so no Rotowire scraping needed.

SELECT
  p.full_name,
  p.editorial_team_abbr  AS mlb_team,
  p.display_position     AS position,
  p.yahoo_status         AS il_status,
  cr.team_name           AS fantasy_team,
  cr.selected_position   AS slot
FROM current_roster cr
JOIN player p ON cr.player_id = p.player_id
WHERE p.yahoo_status IN ('IL7', 'IL10', 'IL15', 'IL60')
  AND cr.selected_position != 'IL'
ORDER BY cr.team_name, p.full_name;
