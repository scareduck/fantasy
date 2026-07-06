# IL Pitcher Triage Tool — Claude Code Spec

## Overview

Extend the existing fantasy baseball free agent finder with a new command or subcommand that identifies IL-stashed pitchers in the local database and uses AI-driven web research to assess whether each pitcher is worth keeping on the fantasy IL slot this season. The tool should produce a triage report: a ranked or categorized list with injury status, expected return timeline, and a Keep / Monitor / Drop recommendation.

---

## Context & Prior Work

This tool lives alongside the existing free agent finder, which already:
- Connects to a local MariaDB instance containing MLB player and roster data
- Has a working DB connection layer (reuse it exactly)
- Outputs results to the terminal (maintain that pattern)

Do not reinvent the DB plumbing. Import or require the existing connection module.

---

## What This Tool Does

1. **Queries the local database** for pitchers currently on the IL
2. **For each IL pitcher**, fires a Claude API call (claude-sonnet-4-20250514) with web search enabled to research:
   - Current injury type and affected body part
   - Most recent news on their recovery status
   - Expected return timeline (specific date range if available, or "post-ASB", "2026 season in doubt", etc.)
   - Whether they've begun a rehab assignment
   - Any red flags (re-injury, surgery, second opinion)
3. **Classifies each pitcher** into one of three buckets:
   - ✅ **KEEP** — credible return path this season, worth holding the IL slot
   - ⚠️ **MONITOR** — uncertain timeline, worth watching but not locking in
   - ❌ **DROP** — season-ending injury, surgery, or no realistic 2026 return
4. **Outputs a formatted triage report** to the terminal

---

## Database Query

The existing DB already tracks IL status. Query for pitchers where:
- `il_status` is active (60-day or standard IL — include both)
- Position is SP or RP (or however pitchers are flagged in the schema)
- Optionally: filter to only players rostered in the user's fantasy league (if that flag exists in the DB)

Use a query pattern consistent with the free agent finder. If the schema uses a `players` table with an `il_status` column and a `position` column, something like:

```sql
SELECT player_id, full_name, team, il_status, il_date
FROM players
WHERE il_status IN ('10-day', '60-day')
  AND position IN ('SP', 'RP', 'P')
ORDER BY il_date ASC;
```

Adjust to match the actual schema. Do a `DESCRIBE players` (or equivalent) before writing the query.

---

## AI Research Layer

For each pitcher returned from the DB query, make a single Claude API call:

```
Model: claude-sonnet-4-20250514
Max tokens: 800
Tools: web_search (type: "web_search_20250305")
```

### System prompt:
```
You are a fantasy baseball analyst specializing in injury assessment. 
Your job is to research a pitcher's current injury status and return timeline 
using the most recent available news. Be concise and direct. 
Always search for the most recent news before answering.
Respond ONLY with a JSON object — no preamble, no markdown, no backticks.
```

### User prompt template:
```
Research the current injury status of MLB pitcher {PLAYER_NAME} ({TEAM}).
They are currently on the {IL_TYPE} IL.

Search for the most recent news about their injury and return timeline.

Return ONLY this JSON structure:
{
  "injury_description": "brief description of injury type and body part",
  "latest_news": "1-2 sentence summary of most recent update",
  "return_timeline": "specific estimate if available, e.g. 'early July', 'post All-Star break', 'season in doubt'",
  "rehab_status": "not started | in progress | completed | N/A",
  "red_flags": "any surgery, setback, or second opinion news — null if none",
  "recommendation": "KEEP | MONITOR | DROP",
  "recommendation_reason": "one sentence explaining the recommendation",
  "source_date": "approximate date of most recent information found"
}
```

### Response handling:
- Parse the JSON from `data.content` (find the text block, strip any accidental markdown fences)
- If parsing fails, mark the pitcher as `MONITOR` with a note that the AI call failed, and log the raw response for debugging
- Do NOT crash the whole run on a single failed parse — continue to the next pitcher

---

## Output Format

Print a formatted report to stdout. Example:

```
═══════════════════════════════════════════════════
  IL PITCHER TRIAGE REPORT  —  June 7, 2026
  12 pitchers evaluated
═══════════════════════════════════════════════════

✅ KEEP (3)
────────────────────────────────────────
  Garrett Crockett  (CWS)  — 60-day IL
  Injury: Left shoulder inflammation
  Timeline: Post All-Star break return targeted
  Rehab: Not started
  Note: No surgery, team expressing optimism for H2 return
  Updated: June 5, 2026

  ...

⚠️  MONITOR (5)
────────────────────────────────────────
  ...

❌ DROP (4)
────────────────────────────────────────
  ...

═══════════════════════════════════════════════════
```

Sort within each bucket by IL date ascending (longest-tenured first).

---

## CLI Interface

Add a new subcommand to the existing tool entry point, e.g.:

```bash
node fantasy.js il-triage
# or
python fantasy.py il-triage
```

Optional flags:
- `--limit N` — only process first N pitchers (useful for testing without burning API calls)
- `--sp-only` — starting pitchers only
- `--verbose` — print raw AI response alongside the formatted output

---

## Rate Limiting & Cost Management

- Process pitchers sequentially, not in parallel — one API call at a time
- Add a brief delay between calls (e.g. 500ms) to avoid rate limits
- The `--limit` flag is important for dev/testing — use `--limit 2` when iterating

---

## Error Handling

- DB connection failure: exit with a clear message ("Could not connect to MariaDB — is the service running?")
- Empty query result: print "No IL pitchers found in database" and exit cleanly
- API key missing: check for `ANTHROPIC_API_KEY` env var at startup and exit with instructions if absent
- Individual AI call failure: log the error, mark pitcher as MONITOR, continue

---

## Files to Create

Follow the same file/folder conventions as the existing free agent finder. Likely:
- `il-triage.js` (or `.py`) — main script
- No new config files needed; reuse existing DB config and API key setup

---

## Notes for Implementation

- Garret Crockett and Cole Ragans are the two known cases as of mid-2026 that prompted this tool. Both have been on the IL most of the season with no clear return date. Use them as the test cases when developing with `--limit 2` (assuming they're in the DB).
- The tool's primary value is saving the time of manually Googling each IL stash and making a judgment call — the AI read should be treated as a starting point, not gospel. The output format should make it easy to scan quickly.
- If the DB schema turns out not to have a clean `il_status` column, check whether there's a separate `transactions` or `roster_status` table that tracks IL placements.
