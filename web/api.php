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

if ($type === 'pitchers') {
    // current_availability = MAX sync_run from player_availability_snapshot (latest P run)
    // current_pitcher_stats = MAX sync_run from pitcher_season_stats
    // They come from consecutive sync_run_ids in the same sync session; join on player_id only.
    $sql = "
        SELECT
            p.full_name                              AS name,
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
