# Yahoo Fantasy Baseball Waiver Tool

Local Python + MariaDB starter project for pulling Yahoo Fantasy Baseball waiver-wire pitchers, preserving historical snapshots, and ranking streamers by category fit instead of generic fantasy points.

## What this starter does

- Authenticates to Yahoo Fantasy Sports with OAuth 2.0
- Discovers the current MLB game key
- Finds your MLB league for the authenticated user
- Pulls available pitchers from your league context
- Stores stable player data in MariaDB
- Stores timestamped availability snapshots in MariaDB
- Writes timestamped CSV audit snapshots, e.g.:
  - `snapshots/free_agents_20260410T064215-0500.csv`
- Imports later enrichment data from CSVs:
  - probable starters / matchup / park
  - projections (including ESPN FPTS if you export or key them in elsewhere)
  - your own tags and notes
- Ranks the latest available pitchers into practical buckets:
  - safe ratio stream
  - K/W chase
  - SV+H dart

## Design choices baked in

- **MariaDB is the source of truth**
- **CSV snapshots are audit/history artifacts**
- **Ranking is category-based**, not fantasy-points based
- **Yahoo % rostered is only a tiebreaker**, not the main signal
- **Minimal dependencies**: `requests` and `mariadb` plus Python stdlib

## Project layout

- `schema.sql` - MariaDB schema
- `.env.example` - environment variables
- `requirements.txt` - Python dependencies
- `scripts/yahoo_sync.py` - Yahoo ingest + CSV snapshot
- `scripts/espn_forecaster_sync.py` - ESPN forecaster ingest + Yahoo player correlation
- `scripts/enrich_pitchers.py` - import probable starters / projections / notes from CSV
- `scripts/rank_streamers.py` - category-based ranking from the DB
- `fantasy/` - shared library code

## Setup

### 1) Create the database

```sql
CREATE DATABASE fantasy CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

Then load the schema:

```bash
mysql -u root -p fantasy < schema.sql
```

### 2) Create a Yahoo developer app

Create a Yahoo developer app with Fantasy Sports private-user access enabled. Put the client ID and secret into your environment. The starter defaults to an out-of-band (`oob`) redirect so you can paste the authorization code back into the terminal.

### 3) Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4) Configure environment

```bash
cp .env.example .env
# edit .env
set -a
source .env
set +a
```

## First run

Run a Yahoo sync:

```bash
python scripts/yahoo_sync.py
```

On first run it will:

- open Yahoo auth in your browser
- ask you to approve the app
- prompt you to paste back the authorization code
- save tokens to `tokens/yahoo_token.json`

Then it will:

- discover the current MLB game key
- find your league key
- fetch available pitchers with `status=FA` and `status=W`
- write a snapshot CSV
- insert snapshot rows into MariaDB

## Helpful flags

```bash
python scripts/yahoo_sync.py --league-key 458.l.12345
python scripts/yahoo_sync.py --statuses FA,W
python scripts/yahoo_sync.py --position P
python scripts/yahoo_sync.py --page-size 25
python scripts/yahoo_sync.py --dry-run
```

## Enrichment imports

## ESPN forecaster sync

This project includes a first-class ESPN forecaster ingestion script for probable starting pitcher rows (next 10 days). It stores the raw row payload in MariaDB, attempts to match each row to existing Yahoo players, and writes both full and unresolved CSV snapshots.

Matching order:

1. `player_external_id` explicit mapping (`source_name=espn_forecaster`)
2. normalized `full_name + editorial_team_abbr`
3. ASCII-normalized name + normalized team abbreviation
4. unresolved / ambiguous output for manual mapping

Run:

```bash
python scripts/espn_forecaster_sync.py
```

Optional:

```bash
python scripts/espn_forecaster_sync.py --dry-run
python scripts/espn_forecaster_sync.py --url "https://www.espn.com/fantasy/baseball/story/_/id/31165100/fantasy-baseball-forecaster-probable-starting-pitcher-projections-matchups-daily-weekly-leagues"
```

Outputs:

- `snapshots/espn_forecaster_<timestamp>.csv`
- `snapshots/espn_forecaster_unresolved_<timestamp>.csv`
- database rows in `espn_forecaster_snapshot` with `raw_row_payload` JSON

Manual mapping workflow:

- Insert rows into `player_external_id` for unresolved ESPN `espn_player_id` values.
- Re-run `scripts/espn_forecaster_sync.py`.
- Rows that now resolve via explicit mapping will be linked to `player.player_id`.

### Probable starters CSV

Expected columns:

- `yahoo_player_key` or (`player_name` + `editorial_team_abbr`)
- `source`
- `start_date` (`YYYY-MM-DD`)
- `opponent_team_abbr`
- `is_home` (`0/1`, `true/false`, `y/n`)
- `park`
- `role`
- `game_time_local` (optional ISO datetime)
- `notes`

Import:

```bash
python scripts/enrich_pitchers.py --probables-csv data/probables.csv
```

### Projections CSV

Expected columns:

- `yahoo_player_key` or (`player_name` + `editorial_team_abbr`)
- `source`
- `projection_date` (`YYYY-MM-DD`)
- `innings`
- `wins`
- `strikeouts`
- `era`
- `whip`
- `sv_holds`
- `espn_fpts`
- `opponent_team_abbr` (optional)
- `park` (optional)
- `notes`

Import:

```bash
python scripts/enrich_pitchers.py --projections-csv data/projections.csv
```

### Notes / tags CSV

Expected columns:

- `yahoo_player_key` or (`player_name` + `editorial_team_abbr`)
- `tag`
- `note_text`
- `source`
- `is_active`

Import:

```bash
python scripts/enrich_pitchers.py --notes-csv data/stream_notes.csv
```

## Ranking

Use the latest sync by default:

```bash
python scripts/rank_streamers.py
```

Or choose a bucket:

```bash
python scripts/rank_streamers.py --bucket safe_ratio
python scripts/rank_streamers.py --bucket kw_chase
python scripts/rank_streamers.py --bucket svh_dart
```

Other options:

```bash
python scripts/rank_streamers.py --limit 15
python scripts/rank_streamers.py --sync-run-id 12
python scripts/rank_streamers.py --league-key 458.l.12345
```

## Schema notes

### Why both `sync_run` and `player_availability_snapshot`?

- `sync_run` records **one pull event**
- `player_availability_snapshot` records **which players were available in that pull**

That gives you the questions you wanted later, such as:

- "What did the wire look like when I added Randy Vásquez?"
- "What were the top available streamers on April 15?"

### Why `roster_move`?

That table is optional, but useful if you later want to tie an add/drop to:

- a Yahoo transaction key
- a `sync_run_id`
- a snapshot CSV filename

## Practical next steps after this starter

1. Add a source adapter for probable starters
2. Add a source adapter for park factors
3. Add your own recurring note/tag taxonomy
4. Refine the ranking formulas based on how your league actually plays
5. Optionally add a report script for “best streamers by date” or “wire state at time of move”

## Notes on ESPN data

The ESPN forecaster ingest is intentionally isolated from the Yahoo availability sync flow. Yahoo sync continues to operate as the core ingestion path, while ESPN rows are stored as separate snapshots for correlation and auditability.
