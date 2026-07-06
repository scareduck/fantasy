# Free Agent Injured List Pitcher Identifier/Scorer/Return Date Finder

## Motivation

There's a long list of free agent pitchers on the IL. What is missing is their
timetable to the majors, and the likelihood of their success upon return. It would be
useful if you could write a script to identify such pitchers, starting with the
extent database tables, then get news articles about them to determine likely return
date (if known, estimate for expressions like "late June" and flag this as an
estimate). Claude AI would take on the task of scoring severity (labrum tear > UCL
injury > hamstring strain, etc.), as well as flagging possible high quality pitchers
with a near return date based on reading through news articles. "High quality" means
demonstrated ability over career or recent years.

## Sources

Sources for news would ideally come from

* The Yahoo Fantasy API, if available, because this would cleanly map the player to the news.
* Otherwise suggest possible related sources including general web searches.

Player availability data (free agent vs. rostered) will come from
player_availability_snapshot or its current status VIEW, current_availability.
Injury status should be available in player.yahoo_status (any status beginning with
IL*). Both these will be found in the fantasy database, the same as other current
reporting/web user interfaces.

## Result

This should result in a new Free Agent IL pitcher page with the existing fantasy web
tools, in a style similar to what is already in place. This should also probably show
YTD stats, if any (ERA, WHIP, K/9), projected return date (and whether this is an
estimate), link to source(s) (possibly with a popup showing multiple if available),
and injury severity score. As with the FA pitcher page, the FA pitcher name should
link to the "Add Free Agent" in an external window, with a "stats" link following the
player name that goes to the Yahoo stats page for that player. All the new
derived/inferred fields should be both orderable and filterable. Data will reside in
a new MariaDB table alongside other extant fantasy (schema name: fantasy) tables.
