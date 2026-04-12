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
    created_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
    updated_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP() ON UPDATE UTC_TIMESTAMP(),
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
    created_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
    updated_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP() ON UPDATE UTC_TIMESTAMP(),
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
    created_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
    updated_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP() ON UPDATE UTC_TIMESTAMP(),
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
    created_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
    updated_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP() ON UPDATE UTC_TIMESTAMP(),
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
    started_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
    completed_at_utc DATETIME NULL,
    requested_position VARCHAR(16) NOT NULL DEFAULT 'P',
    requested_statuses VARCHAR(64) NOT NULL,
    snapshot_file VARCHAR(512) NULL,
    row_count INT NOT NULL DEFAULT 0,
    notes TEXT NULL,
    created_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
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
    created_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
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
    captured_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
    created_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
    updated_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP() ON UPDATE UTC_TIMESTAMP(),
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
    captured_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
    created_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
    updated_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP() ON UPDATE UTC_TIMESTAMP(),
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
    captured_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
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
    created_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
    updated_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP() ON UPDATE UTC_TIMESTAMP(),
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
    created_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
    updated_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP() ON UPDATE UTC_TIMESTAMP(),
    PRIMARY KEY (stream_note_id),
    KEY idx_stream_note_player (player_id),
    KEY idx_stream_note_tag_active (tag, is_active),
    CONSTRAINT fk_stream_note_player
        FOREIGN KEY (player_id) REFERENCES player (player_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS roster_move (
    roster_move_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    league_id BIGINT UNSIGNED NOT NULL,
    move_ts_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
    action_type VARCHAR(32) NOT NULL,
    added_player_id BIGINT UNSIGNED NULL,
    dropped_player_id BIGINT UNSIGNED NULL,
    yahoo_transaction_key VARCHAR(64) NULL,
    sync_run_id BIGINT UNSIGNED NULL,
    snapshot_file VARCHAR(512) NULL,
    note_text TEXT NULL,
    created_at_utc DATETIME NOT NULL DEFAULT UTC_TIMESTAMP(),
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
