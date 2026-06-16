CREATE TABLE IF NOT EXISTS league (
    league_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    yahoo_league_key VARCHAR(64) NOT NULL,
    yahoo_game_key VARCHAR(32) NULL,
    game_code VARCHAR(16) NULL,
    season SMALLINT NULL,
    league_name VARCHAR(255) NOT NULL,
    scoring_type VARCHAR(32) NULL,
    num_teams SMALLINT NULL,
    league_url VARCHAR(512) NULL,
    last_synced_at_utc DATETIME NULL,
    raw_settings_xml LONGTEXT NULL,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (league_id),
    UNIQUE KEY uq_league_yahoo_key (yahoo_league_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS league_stat_category (
    league_stat_category_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    league_id BIGINT UNSIGNED NOT NULL,
    stat_id INT NOT NULL,
    stat_name VARCHAR(128) NOT NULL,
    display_name VARCHAR(128) NULL,
    position_type VARCHAR(8) NULL,
    sort_order TINYINT NULL,
    is_enabled TINYINT(1) NOT NULL DEFAULT 1,
    is_focus_category TINYINT(1) NOT NULL DEFAULT 0,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (league_stat_category_id),
    UNIQUE KEY uq_league_stat (league_id, stat_id),
    CONSTRAINT fk_league_stat_category_league
        FOREIGN KEY (league_id) REFERENCES league (league_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS player (
    player_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    yahoo_player_key VARCHAR(64) NOT NULL,
    yahoo_player_id BIGINT NULL,
    editorial_player_key VARCHAR(64) NULL,
    full_name VARCHAR(255) NOT NULL,
    first_name VARCHAR(128) NULL,
    last_name VARCHAR(128) NULL,
    ascii_first_name VARCHAR(128) NULL,
    ascii_last_name VARCHAR(128) NULL,
    editorial_team_key VARCHAR(64) NULL,
    editorial_team_full_name VARCHAR(128) NULL,
    editorial_team_abbr VARCHAR(16) NULL,
    uniform_number VARCHAR(16) NULL,
    display_position VARCHAR(64) NULL,
    position_type VARCHAR(8) NULL,
    eligible_positions_json JSON NULL,
    yahoo_status VARCHAR(64) NULL,
    yahoo_status_full VARCHAR(255) NULL,
    raw_player_xml LONGTEXT NULL,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (player_id),
    UNIQUE KEY uq_player_yahoo_key (yahoo_player_key),
    KEY idx_player_name (full_name),
    KEY idx_player_team_name (editorial_team_abbr, full_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS player_external_id (
    player_external_id_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    player_id BIGINT UNSIGNED NOT NULL,
    source_name VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    external_label VARCHAR(255) NULL,
    team_abbr VARCHAR(16) NULL,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (player_external_id_id),
    UNIQUE KEY uq_external_source_id (source_name, external_id),
    KEY idx_external_player_source (player_id, source_name),
    CONSTRAINT fk_player_external_id_player
        FOREIGN KEY (player_id) REFERENCES player (player_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS sync_run (
    sync_run_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    league_id BIGINT UNSIGNED NOT NULL,
    source_name VARCHAR(32) NOT NULL DEFAULT 'yahoo',
    started_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at_utc DATETIME NULL,
    requested_position VARCHAR(16) NOT NULL DEFAULT 'P',
    requested_statuses VARCHAR(64) NOT NULL,
    snapshot_file VARCHAR(512) NULL,
    row_count INT NOT NULL DEFAULT 0,
    notes TEXT NULL,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sync_run_id),
    KEY idx_sync_run_league_started (league_id, started_at_utc),
    CONSTRAINT fk_sync_run_league
        FOREIGN KEY (league_id) REFERENCES league (league_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS player_availability_snapshot (
    snapshot_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    sync_run_id BIGINT UNSIGNED NOT NULL,
    league_id BIGINT UNSIGNED NOT NULL,
    player_id BIGINT UNSIGNED NOT NULL,
    captured_at_utc DATETIME NOT NULL,
    availability_status VARCHAR(8) NOT NULL,
    source_page_start INT NOT NULL DEFAULT 0,
    source_page_count INT NOT NULL DEFAULT 0,
    percent_owned DECIMAL(5,2) NULL,
    raw_player_xml LONGTEXT NULL,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (snapshot_id),
    UNIQUE KEY uq_snapshot_sync_player_status (sync_run_id, player_id, availability_status),
    KEY idx_snapshot_league_captured (league_id, captured_at_utc),
    KEY idx_snapshot_player_captured (player_id, captured_at_utc),
    CONSTRAINT fk_snapshot_sync_run
        FOREIGN KEY (sync_run_id) REFERENCES sync_run (sync_run_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_snapshot_league
        FOREIGN KEY (league_id) REFERENCES league (league_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_snapshot_player
        FOREIGN KEY (player_id) REFERENCES player (player_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS probable_start (
    probable_start_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    player_id BIGINT UNSIGNED NOT NULL,
    source_name VARCHAR(64) NOT NULL,
    start_date DATE NOT NULL,
    opponent_team_abbr VARCHAR(16) NULL,
    is_home TINYINT(1) NULL,
    park VARCHAR(128) NULL,
    role_code VARCHAR(32) NULL,
    game_time_local DATETIME NULL,
    notes TEXT NULL,
    captured_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (probable_start_id),
    UNIQUE KEY uq_probable_start (player_id, source_name, start_date),
    KEY idx_probable_start_date (start_date),
    CONSTRAINT fk_probable_start_player
        FOREIGN KEY (player_id) REFERENCES player (player_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS projection (
    projection_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    player_id BIGINT UNSIGNED NOT NULL,
    source_name VARCHAR(64) NOT NULL,
    projection_date DATE NOT NULL,
    innings DECIMAL(6,2) NULL,
    wins DECIMAL(6,2) NULL,
    strikeouts DECIMAL(6,2) NULL,
    era DECIMAL(6,3) NULL,
    whip DECIMAL(6,3) NULL,
    sv_holds DECIMAL(6,2) NULL,
    espn_fpts DECIMAL(8,2) NULL,
    opponent_team_abbr VARCHAR(16) NULL,
    park VARCHAR(128) NULL,
    notes TEXT NULL,
    captured_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (projection_id),
    UNIQUE KEY uq_projection (player_id, source_name, projection_date),
    KEY idx_projection_date (projection_date),
    CONSTRAINT fk_projection_player
        FOREIGN KEY (player_id) REFERENCES player (player_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS espn_forecaster_snapshot (
    espn_forecaster_snapshot_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    source_name VARCHAR(64) NOT NULL DEFAULT 'espn_forecaster',
    captured_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    forecaster_for_date VARCHAR(64) NULL,
    espn_player_id VARCHAR(64) NULL,
    pitcher_name VARCHAR(255) NOT NULL,
    team_abbr VARCHAR(16) NULL,
    opponent_team_abbr VARCHAR(16) NULL,
    matchup_text VARCHAR(255) NULL,
    projection_text VARCHAR(255) NULL,
    player_id BIGINT UNSIGNED NULL,
    match_method VARCHAR(64) NOT NULL DEFAULT 'unresolved',
    raw_row_payload JSON NOT NULL,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (espn_forecaster_snapshot_id),
    KEY idx_espn_forecaster_captured (captured_at_utc),
    KEY idx_espn_forecaster_player (player_id),
    KEY idx_espn_forecaster_espn_player (espn_player_id),
    CONSTRAINT fk_espn_forecaster_player
        FOREIGN KEY (player_id) REFERENCES player (player_id)
        ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stream_note (
    stream_note_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    player_id BIGINT UNSIGNED NOT NULL,
    tag VARCHAR(64) NOT NULL,
    note_text TEXT NULL,
    source_name VARCHAR(64) NOT NULL DEFAULT 'manual',
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (stream_note_id),
    KEY idx_stream_note_player (player_id),
    KEY idx_stream_note_tag_active (tag, is_active),
    CONSTRAINT fk_stream_note_player
        FOREIGN KEY (player_id) REFERENCES player (player_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS roster_snapshot (
    roster_snapshot_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    league_id BIGINT UNSIGNED NOT NULL,
    team_key VARCHAR(64) NOT NULL,
    team_name VARCHAR(255) NULL,
    player_id BIGINT UNSIGNED NULL,
    yahoo_player_key VARCHAR(64) NOT NULL,
    selected_position VARCHAR(16) NULL,
    captured_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (roster_snapshot_id),
    KEY idx_roster_snapshot_league (league_id, captured_at_utc),
    KEY idx_roster_snapshot_player (player_id),
    CONSTRAINT fk_roster_snapshot_league
        FOREIGN KEY (league_id) REFERENCES league (league_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_roster_snapshot_player
        FOREIGN KEY (player_id) REFERENCES player (player_id)
        ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS roster_move (
    roster_move_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    league_id BIGINT UNSIGNED NOT NULL,
    move_ts_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    action_type VARCHAR(32) NOT NULL,
    added_player_id BIGINT UNSIGNED NULL,
    dropped_player_id BIGINT UNSIGNED NULL,
    yahoo_transaction_key VARCHAR(64) NULL,
    sync_run_id BIGINT UNSIGNED NULL,
    snapshot_file VARCHAR(512) NULL,
    note_text TEXT NULL,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (roster_move_id),
    KEY idx_roster_move_league_ts (league_id, move_ts_utc),
    CONSTRAINT fk_roster_move_league
        FOREIGN KEY (league_id) REFERENCES league (league_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_roster_move_added_player
        FOREIGN KEY (added_player_id) REFERENCES player (player_id)
        ON DELETE SET NULL,
    CONSTRAINT fk_roster_move_dropped_player
        FOREIGN KEY (dropped_player_id) REFERENCES player (player_id)
        ON DELETE SET NULL,
    CONSTRAINT fk_roster_move_sync_run
        FOREIGN KEY (sync_run_id) REFERENCES sync_run (sync_run_id)
        ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS batter_season_stats (
    batter_season_stats_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    sync_run_id BIGINT UNSIGNED NOT NULL,
    player_id BIGINT UNSIGNED NOT NULL,
    captured_at_utc DATETIME NOT NULL,
    ab INT NULL,
    r INT NULL,
    h INT NULL,
    hr INT NULL,
    rbi INT NULL,
    sb INT NULL,
    bb INT NULL,
    obp DECIMAL(5,3) NULL,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (batter_season_stats_id),
    UNIQUE KEY uq_batter_stats (sync_run_id, player_id),
    KEY idx_batter_stats_player (player_id),
    CONSTRAINT fk_batter_stats_sync_run
        FOREIGN KEY (sync_run_id) REFERENCES sync_run (sync_run_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_batter_stats_player
        FOREIGN KEY (player_id) REFERENCES player (player_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS pitcher_season_stats (
    pitcher_season_stats_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    sync_run_id BIGINT UNSIGNED NOT NULL,
    player_id BIGINT UNSIGNED NOT NULL,
    captured_at_utc DATETIME NOT NULL,
    ip DECIMAL(6,2) NULL,
    w INT NULL,
    k INT NULL,
    era DECIMAL(6,3) NULL,
    whip DECIMAL(6,3) NULL,
    sv_holds DECIMAL(6,2) NULL,
    created_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pitcher_season_stats_id),
    UNIQUE KEY uq_pitcher_stats (sync_run_id, player_id),
    KEY idx_pitcher_stats_player (player_id),
    CONSTRAINT fk_pitcher_stats_sync_run
        FOREIGN KEY (sync_run_id) REFERENCES sync_run (sync_run_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_pitcher_stats_player
        FOREIGN KEY (player_id) REFERENCES player (player_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS pitcher_game_log (
    pitcher_game_log_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    player_id BIGINT UNSIGNED NOT NULL,
    game_date DATE NOT NULL,
    sv_holds DECIMAL(6,2) NOT NULL DEFAULT 0,
    captured_at_utc DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pitcher_game_log_id),
    KEY idx_pitcher_game_log_player_date (player_id, game_date),
    KEY idx_pitcher_game_log_date (game_date),
    CONSTRAINT fk_pitcher_game_log_player
        FOREIGN KEY (player_id) REFERENCES player (player_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Views must be created with utf8mb4_unicode_ci to match the base table
-- collation. Without this, PHP's set_charset() locks prepared-statement
-- parameters to utf8mb4_general_ci, causing LIKE/UNION collation errors.
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Current roster: the most recent roster snapshot across all teams.
CREATE OR REPLACE VIEW current_roster AS
SELECT
    rs.roster_snapshot_id,
    rs.league_id,
    rs.team_key,
    rs.team_name,
    rs.player_id,
    rs.yahoo_player_key,
    rs.selected_position,
    rs.captured_at_utc
FROM roster_snapshot rs
WHERE rs.captured_at_utc = (SELECT MAX(captured_at_utc) FROM roster_snapshot);

-- Current ESPN forecaster: the most recent forecaster snapshot.
CREATE OR REPLACE VIEW current_espn_forecast AS
SELECT
    efs.espn_forecaster_snapshot_id,
    efs.source_name,
    efs.captured_at_utc,
    efs.forecaster_for_date,
    efs.espn_player_id,
    efs.pitcher_name,
    efs.team_abbr,
    efs.opponent_team_abbr,
    efs.matchup_text,
    efs.projection_text,
    efs.player_id,
    efs.match_method,
    efs.raw_row_payload,
    efs.created_at_utc,
    efs.updated_at_utc
FROM espn_forecaster_snapshot efs
WHERE efs.captured_at_utc = (SELECT MAX(captured_at_utc) FROM espn_forecaster_snapshot);

-- Current pitcher season stats: the most recent all-pitcher stats sync (status=A,T).
CREATE OR REPLACE VIEW current_pitcher_stats AS
SELECT
    pss.pitcher_season_stats_id,
    pss.sync_run_id,
    pss.player_id,
    pss.captured_at_utc,
    pss.ip,
    pss.w,
    pss.k,
    pss.era,
    pss.whip,
    pss.sv_holds,
    pss.created_at_utc
FROM pitcher_season_stats pss
WHERE pss.sync_run_id = (
    SELECT MAX(sync_run_id)
    FROM sync_run
    WHERE requested_position = 'P'
      AND requested_statuses = 'A,T'
);

-- Current player availability: the most recent pitcher (P) availability snapshot.
-- Scoped to P runs so a subsequent batter sync does not displace pitcher availability.
CREATE OR REPLACE VIEW current_availability AS
SELECT
    pas.snapshot_id,
    pas.sync_run_id,
    pas.league_id,
    pas.player_id,
    pas.captured_at_utc,
    pas.availability_status,
    pas.source_page_start,
    pas.source_page_count,
    pas.percent_owned,
    pas.raw_player_xml,
    pas.created_at_utc
FROM player_availability_snapshot pas
WHERE pas.sync_run_id = (
    SELECT MAX(pas2.sync_run_id)
    FROM player_availability_snapshot pas2
    JOIN sync_run sr ON sr.sync_run_id = pas2.sync_run_id
    WHERE sr.requested_position = 'P'
);

-- MLB game schedule with probable starters (one row per game).
CREATE TABLE IF NOT EXISTS mlb_schedule (
    mlb_schedule_id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    game_pk                  INT UNSIGNED    NOT NULL,
    game_date                DATE            NOT NULL,
    game_datetime_utc        DATETIME,
    home_team_abbr           VARCHAR(16)     NOT NULL,
    away_team_abbr           VARCHAR(16)     NOT NULL,
    home_pitcher_name        VARCHAR(255),
    home_pitcher_mlb_id      INT UNSIGNED,
    home_pitcher_player_id   BIGINT UNSIGNED,
    away_pitcher_name        VARCHAR(255),
    away_pitcher_mlb_id      INT UNSIGNED,
    away_pitcher_player_id   BIGINT UNSIGNED,
    captured_at_utc          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_game_pk    (game_pk),
    KEY idx_game_date        (game_date)
);

-- Rotowire injury snapshot: one row per player per fetch.
CREATE TABLE IF NOT EXISTS rotowire_injury_snapshot (
    rotowire_injury_snapshot_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    captured_at_utc             DATETIME        NOT NULL,
    rotowire_player_id          INT UNSIGNED,
    player_name                 VARCHAR(255)    NOT NULL,
    team                        VARCHAR(16),
    position                    VARCHAR(32),
    injury                      VARCHAR(255),
    status                      VARCHAR(64),
    r_date                      VARCHAR(32),
    rotowire_url                VARCHAR(255),
    KEY idx_rw_captured         (captured_at_utc),
    KEY idx_rw_player           (player_name)
);

-- Current Rotowire injuries: most recent snapshot batch.
CREATE OR REPLACE VIEW current_rotowire_injuries AS
SELECT * FROM rotowire_injury_snapshot
WHERE captured_at_utc = (SELECT MAX(captured_at_utc) FROM rotowire_injury_snapshot);

-- FA IL pitcher AI analysis: one row per player, upserted on each analysis run.
CREATE TABLE IF NOT EXISTS fa_il_pitcher_analysis (
    fa_il_pitcher_analysis_id BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT PRIMARY KEY,
    player_id               BIGINT UNSIGNED    NOT NULL UNIQUE,
    injury_description      TEXT,
    injury_severity         TINYINT,
    return_date             DATE,
    return_date_is_estimate TINYINT(1)         NOT NULL DEFAULT 0,
    is_high_quality         TINYINT(1)         NOT NULL DEFAULT 0,
    quality_notes           TEXT,
    return_notes            TEXT,
    news_sources_json       TEXT,
    analyzed_at_utc         DATETIME           NOT NULL,
    created_at_utc          DATETIME           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at_utc          DATETIME           NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_fail_player FOREIGN KEY (player_id) REFERENCES player (player_id)
);

-- Batter-centric schedule view: one row per team per game with opposing pitcher.
CREATE OR REPLACE VIEW mlb_batter_matchup AS
SELECT
    game_date,
    away_team_abbr                                                        AS batter_team,
    home_team_abbr                                                        AS pitcher_team,
    0                                                                     AS is_home,
    home_pitcher_name                                                     AS opp_pitcher_name,
    home_pitcher_mlb_id                                                   AS opp_pitcher_mlb_id,
    home_pitcher_player_id                                                AS opp_pitcher_player_id,
    CONCAT(DATE_FORMAT(game_date, '%a %c/%e'), '-@', home_team_abbr)     AS matchup_text
FROM mlb_schedule
UNION ALL
SELECT
    game_date,
    home_team_abbr                                                        AS batter_team,
    away_team_abbr                                                        AS pitcher_team,
    1                                                                     AS is_home,
    away_pitcher_name                                                     AS opp_pitcher_name,
    away_pitcher_mlb_id                                                   AS opp_pitcher_mlb_id,
    away_pitcher_player_id                                                AS opp_pitcher_player_id,
    CONCAT(DATE_FORMAT(game_date, '%a %c/%e'), '-', away_team_abbr)      AS matchup_text
FROM mlb_schedule;

-- Current batter season stats: the most recent batter stats sync.
CREATE OR REPLACE VIEW current_batter_stats AS
SELECT
    bss.batter_season_stats_id,
    bss.sync_run_id,
    bss.player_id,
    bss.captured_at_utc,
    bss.ab,
    bss.r,
    bss.h,
    bss.hr,
    bss.rbi,
    bss.sb,
    bss.bb,
    bss.obp,
    bss.created_at_utc
FROM batter_season_stats bss
WHERE bss.sync_run_id = (SELECT MAX(sync_run_id) FROM batter_season_stats);
