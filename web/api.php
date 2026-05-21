<?php
header('Content-Type: application/json');
header('Cache-Control: no-cache');

$type = isset($_GET['type']) ? $_GET['type'] : 'batters';

$conn = new mysqli('127.0.0.1', 'rlm', '', 'fantasy', 3306);
if ($conn->connect_error) {
    http_response_code(500);
    echo json_encode(['error' => $conn->connect_error]);
    exit;
}
$conn->set_charset('utf8mb4');

if ($type === 'svh') {
    // Validate optional team filter against live roster data.
    $filter_team = null;
    if (isset($_GET['team']) && $_GET['team'] !== '' && $_GET['team'] !== 'all') {
        $chk = $conn->prepare("SELECT 1 FROM current_roster WHERE team_key = ? LIMIT 1");
        $chk->bind_param('s', $_GET['team']);
        $chk->execute();
        if ($chk->get_result()->num_rows > 0) $filter_team = $_GET['team'];
        $chk->close();
    }
    $team_clause = $filter_team
        ? " AND cr.team_key = '" . $conn->real_escape_string($filter_team) . "'"
        : "";

    $sql = "
        SELECT
            p.full_name                                     AS name,
            p.yahoo_player_key                              AS player_key,
            p.editorial_team_abbr                           AS team,
            CAST(cps.sv_holds AS UNSIGNED)                  AS season_svh,
            MAX(pgl.game_date)                              AS last_svh_date,
            DATEDIFF(CURDATE(), MAX(pgl.game_date))         AS days_since,
            CAST(cps.era  AS DECIMAL(5,2))                  AS era,
            CAST(cps.whip AS DECIMAL(5,3))                  AS whip,
            COALESCE(ca.availability_status, 'T')           AS avail_status,
            COALESCE(p.yahoo_status, '')                    AS yahoo_status,
            cr.team_key                                     AS fantasy_team_key,
            cr.team_name                                    AS fantasy_team_name
        FROM current_pitcher_stats cps
        JOIN  player p ON p.player_id = cps.player_id
        LEFT JOIN pitcher_game_log pgl ON pgl.player_id = cps.player_id
        LEFT JOIN (
            SELECT player_id, MAX(availability_status) AS availability_status
            FROM   current_availability
            GROUP BY player_id
        ) ca ON ca.player_id = cps.player_id
        LEFT JOIN current_roster cr ON cr.player_id = cps.player_id
        WHERE cps.sv_holds >= 1
        $team_clause
        GROUP BY
            p.player_id, p.full_name, p.editorial_team_abbr,
            p.yahoo_player_key, cps.sv_holds, cps.era, cps.whip, ca.availability_status,
            p.yahoo_status, cr.team_key, cr.team_name
        ORDER BY
            CASE WHEN MAX(pgl.game_date) IS NULL THEN 1 ELSE 0 END,
            DATEDIFF(CURDATE(), MAX(pgl.game_date)),
            cps.sv_holds DESC
    ";
    $result = $conn->query($sql);
    if (!$result) { http_response_code(500); echo json_encode(['error' => $conn->error]); exit; }
    $rows = [];
    while ($row = $result->fetch_assoc()) {
        $row['season_svh'] = $row['season_svh'] !== null ? (int)$row['season_svh'] : null;
        $row['days_since'] = $row['days_since'] !== null ? (int)$row['days_since'] : null;
        $row['era']        = $row['era']        !== null ? (float)$row['era']      : null;
        $row['whip']       = $row['whip']       !== null ? (float)$row['whip']     : null;
        $rows[] = $row;
    }
    $conn->close();
    echo json_encode($rows, JSON_INVALID_UTF8_SUBSTITUTE);
    exit;
}

if ($type === 'teams') {
    $result = $conn->query("SELECT DISTINCT team_key, team_name FROM current_roster ORDER BY team_name");
    $rows = [];
    while ($row = $result->fetch_assoc()) $rows[] = $row;
    $conn->close();
    echo json_encode($rows, JSON_INVALID_UTF8_SUBSTITUTE);
    exit;
}

if ($type === 'meta') {
    $brow = $conn->query("SELECT started_at_utc FROM sync_run WHERE requested_position='B' ORDER BY sync_run_id DESC LIMIT 1")->fetch_assoc();
    $prow = $conn->query("SELECT started_at_utc FROM sync_run WHERE requested_position='P' ORDER BY sync_run_id DESC LIMIT 1")->fetch_assoc();
    echo json_encode([
        'batter_sync'  => $brow ? substr($brow['started_at_utc'], 0, 16) : null,
        'pitcher_sync' => $prow ? substr($prow['started_at_utc'], 0, 16) : null,
    ]);
    $conn->close();
    exit;
}

// Resolve team_key from ?team= param; validate against DB, default to Rob's team.
$team_key = '469.l.73362.t.8';
if (isset($_GET['team'])) {
    $candidate = $_GET['team'];
    $chk = $conn->prepare("SELECT 1 FROM current_roster WHERE team_key = ? LIMIT 1");
    $chk->bind_param('s', $candidate);
    $chk->execute();
    if ($chk->get_result()->num_rows > 0) $team_key = $candidate;
    $chk->close();
}

if ($type === 'starts') {
    $stmt = $conn->prepare("
        SELECT
            DATE_FORMAT(
                STR_TO_DATE(
                    CONCAT(SUBSTRING_INDEX(efs.matchup_text, '-', 1), ' ', YEAR(CURDATE())),
                    '%a %c/%e %Y'
                ), '%Y-%m-%d'
            )                                           AS start_date,
            p.full_name                                 AS name,
            p.yahoo_player_key                          AS player_key,
            p.editorial_team_abbr                       AS mlb_team,
            cr.selected_position                        AS slot,
            efs.matchup_text                            AS start,
            CAST(efs.projection_text AS DECIMAL(6,2))   AS fpts
        FROM current_roster cr
        JOIN player p ON p.player_id = cr.player_id
        JOIN current_espn_forecast efs ON efs.player_id = cr.player_id
        WHERE efs.projection_text IS NOT NULL
          AND cr.team_key = ?
          AND STR_TO_DATE(
                CONCAT(SUBSTRING_INDEX(efs.matchup_text, '-', 1), ' ', YEAR(CURDATE())),
                '%a %c/%e %Y'
              ) >= CURDATE()
        ORDER BY start_date, fpts DESC
    ");
    $stmt->bind_param('s', $team_key);
    $stmt->execute();
    $result = $stmt->get_result();
    $rows = [];
    while ($row = $result->fetch_assoc()) {
        $row['fpts'] = $row['fpts'] !== null ? (float)$row['fpts'] : null;
        $rows[] = $row;
    }
    $conn->close();
    echo json_encode($rows, JSON_INVALID_UTF8_SUBSTITUTE);
    exit;
}

if ($type === 'matchups') {
    $stmt = $conn->prepare("
        SELECT
            p.full_name                                 AS batter,
            p.yahoo_player_key                          AS player_key,
            p.editorial_team_abbr                       AS batter_team,
            cr.selected_position                        AS slot,
            opp.pitcher_name                            AS opp_pitcher,
            opp_p.yahoo_player_key                      AS opp_player_key,
            opp.team_abbr                               AS pitcher_team,
            opp.matchup_text                            AS matchup,
            CAST(opp.projection_text AS DECIMAL(6,2))   AS fpts,
            CAST(cps.era AS DECIMAL(6,2))               AS era
        FROM current_roster cr
        JOIN player p ON p.player_id = cr.player_id
        LEFT JOIN current_espn_forecast opp
            ON  opp.opponent_team_abbr = p.editorial_team_abbr
            AND opp.matchup_text LIKE CONCAT('%', MONTH(CURDATE()), '/', DAY(CURDATE()), '%')
            AND opp.projection_text IS NOT NULL
        LEFT JOIN player opp_p ON opp_p.player_id = opp.player_id
        LEFT JOIN current_pitcher_stats cps ON cps.player_id = opp.player_id
        WHERE p.position_type = 'B'
          AND cr.selected_position != 'IL'
          AND cr.team_key = ?
        ORDER BY FIELD(cr.selected_position, 'C','1B','2B','3B','SS','OF','Util','BN'),
                 fpts DESC
    ");
    $stmt->bind_param('s', $team_key);
    $stmt->execute();
    $result = $stmt->get_result();
    $rows = [];
    while ($row = $result->fetch_assoc()) {
        $row['fpts'] = $row['fpts'] !== null ? (float)$row['fpts'] : null;
        $row['era']  = $row['era']  !== null ? (float)$row['era']  : null;
        $rows[] = $row;
    }
    $conn->close();
    echo json_encode($rows, JSON_INVALID_UTF8_SUBSTITUTE);
    exit;
}

if ($type === 'lineup') {
    $sql = "
        SELECT
            cr.team_key,
            cr.team_name,
            p.full_name                                                              AS name,
            p.editorial_team_abbr                                                    AS mlb_team,
            cr.selected_position                                                     AS roster_pos,
            COALESCE(p.yahoo_status, '')                                             AS status,
            GROUP_CONCAT(ef.matchup_text    ORDER BY ef.matchup_text SEPARATOR '|') AS matchups,
            GROUP_CONCAT(ef.projection_text ORDER BY ef.matchup_text SEPARATOR '|') AS projections,
            CAST(COALESCE(SUM(CAST(ef.projection_text AS DECIMAL(6,1))), 0)
                 AS DECIMAL(6,1))                                                    AS total_fpts,
            COUNT(ef.espn_forecaster_snapshot_id)                                   AS start_count,
            MAX(ef.forecaster_for_date)                                             AS period
        FROM current_roster cr
        JOIN player p ON p.player_id = cr.player_id
        LEFT JOIN current_espn_forecast ef ON ef.player_id = cr.player_id
        WHERE p.position_type = 'P'
        GROUP BY cr.team_key, cr.team_name, p.player_id, p.full_name, p.editorial_team_abbr,
                 cr.selected_position, p.yahoo_status
        ORDER BY cr.team_name, total_fpts DESC
    ";
    $result = $conn->query($sql);
    if (!$result) { http_response_code(500); echo json_encode(['error' => $conn->error]); exit; }
    $rows = [];
    while ($row = $result->fetch_assoc()) {
        $row['total_fpts']  = (float)$row['total_fpts'];
        $row['start_count'] = (int)$row['start_count'];
        $rows[] = $row;
    }
    $conn->close();
    echo json_encode($rows, JSON_INVALID_UTF8_SUBSTITUTE);
    exit;
}

if ($type === 'sp1' || $type === 'sp2') {
    $having   = $type === 'sp1'
        ? 'HAVING COUNT(*) = 1'
        : 'HAVING COUNT(*) >= 2';
    if ($type === 'sp1') {
        $subq_cols  = "MIN(matchup_text) AS matchup,
                       CAST(MIN(projection_text) AS DECIMAL(6,2)) AS fpts";
        $outer_cols = "agg.matchup, agg.fpts";
    } else {
        $subq_cols  = "GROUP_CONCAT(matchup_text ORDER BY matchup_text SEPARATOR ', ') AS starts,
                       ROUND(SUM(CAST(projection_text AS DECIMAL(6,2))), 1) AS fpts";
        $outer_cols = "agg.starts, agg.fpts";
    }
    $sql = "
        SELECT
            p.full_name                              AS name,
            p.yahoo_player_key                       AS player_key,
            p.editorial_team_abbr                    AS team,
            COALESCE(p.yahoo_status, '')              AS status,
            ca.availability_status                   AS avail,
            ca.percent_owned                         AS pct_own,
            $outer_cols,
            CAST(cps.era  AS DECIMAL(5,2))           AS era,
            CAST(cps.whip AS DECIMAL(5,3))           AS whip,
            ROUND(cps.k * 9.0 / NULLIF(cps.ip, 0), 1) AS k9
        FROM current_availability ca
        JOIN player p ON p.player_id = ca.player_id
        JOIN current_pitcher_stats cps ON cps.player_id = ca.player_id
        JOIN (
            SELECT player_id, $subq_cols
            FROM current_espn_forecast
            WHERE player_id IS NOT NULL
              AND STR_TO_DATE(
                    CONCAT(SUBSTRING_INDEX(matchup_text, '-', 1), ' ', YEAR(CURDATE())),
                    '%a %c/%e %Y'
                  ) >= CURDATE()
            GROUP BY player_id
            $having
        ) agg ON agg.player_id = ca.player_id
        ORDER BY fpts DESC
    ";
    $result = $conn->query($sql);
    if (!$result) { http_response_code(500); echo json_encode(['error' => $conn->error]); exit; }
    $rows = [];
    while ($row = $result->fetch_assoc()) {
        $row['fpts'] = $row['fpts'] !== null ? (float)$row['fpts'] : null;
        $row['era']  = $row['era']  !== null ? (float)$row['era']  : null;
        $row['whip'] = $row['whip'] !== null ? (float)$row['whip'] : null;
        $row['k9']   = $row['k9']   !== null ? (float)$row['k9']   : null;
        $rows[] = $row;
    }
    $conn->close();
    echo json_encode($rows, JSON_INVALID_UTF8_SUBSTITUTE);
    exit;
}

if ($type === 'pitchers') {
    // current_availability = MAX sync_run from player_availability_snapshot (latest P run)
    // current_pitcher_stats = MAX sync_run from pitcher_season_stats
    // They come from consecutive sync_run_ids in the same sync session; join on player_id only.
    $sql = "
        SELECT
            p.full_name                              AS name,
            p.yahoo_player_key                       AS player_key,
            p.editorial_team_abbr                    AS team,
            p.display_position                       AS pos,
            COALESCE(p.yahoo_status, '')              AS status,
            ca.availability_status                   AS avail,
            ca.percent_owned                         AS pct_own,
            cps.ip,
            cps.w,
            cps.k,
            CAST(cps.era  AS DECIMAL(5,2))           AS era,
            CAST(cps.whip AS DECIMAL(5,3))           AS whip,
            ROUND(cps.k * 9.0 / NULLIF(cps.ip, 0), 1) AS k9,
            cps.sv_holds
        FROM current_availability ca
        JOIN player p  ON p.player_id  = ca.player_id
        JOIN current_pitcher_stats cps ON cps.player_id = ca.player_id
        ORDER BY cps.era ASC, cps.ip DESC
    ";
} else {
    // Batter availability and stats share the same sync_run_id (B runs).
    $sql = "
        SELECT
            p.full_name                              AS name,
            p.yahoo_player_key                       AS player_key,
            p.editorial_team_abbr                    AS team,
            p.display_position                       AS pos,
            COALESCE(p.yahoo_status, '')              AS status,
            pas.availability_status                  AS avail,
            pas.percent_owned                        AS pct_own,
            bss.ab,
            bss.obp,
            bss.r,
            bss.hr,
            bss.rbi,
            bss.sb,
            bss.bb
        FROM player_availability_snapshot pas
        JOIN player p   ON p.player_id  = pas.player_id
        JOIN batter_season_stats bss
             ON  bss.player_id   = pas.player_id
             AND bss.sync_run_id = pas.sync_run_id
        WHERE pas.sync_run_id = (
                SELECT MAX(sr.sync_run_id)
                FROM   sync_run sr
                WHERE  sr.requested_position = 'B'
              )
        ORDER BY bss.obp DESC, bss.ab DESC
    ";
}

$result = $conn->query($sql);
if (!$result) {
    http_response_code(500);
    echo json_encode(['error' => $conn->error]);
    exit;
}

$rows = [];
while ($row = $result->fetch_assoc()) {
    // Cast numeric strings to numbers for proper JSON types
    foreach ($row as $k => $v) {
        if ($v !== null && $k !== 'name' && $k !== 'team' && $k !== 'pos' && $k !== 'status' && $k !== 'avail') {
            $row[$k] = is_numeric($v) ? $v + 0 : $v;
        }
    }
    $rows[] = $row;
}
$conn->close();
echo json_encode($rows);
