<?php
// Server-Sent Events endpoint: runs the sync pipeline and streams output.
header('Content-Type: text/event-stream');
header('Cache-Control: no-cache');
header('X-Accel-Buffering: no');   // disable nginx/proxy buffering if any
set_time_limit(600);
ini_set('output_buffering', 'off');

function sse(string $data, string $event = 'line'): void {
    echo "event: $event\n";
    echo 'data: ' . json_encode($data) . "\n\n";
    if (ob_get_level()) ob_flush();
    flush();
}

$script = realpath(__DIR__ . '/sync.sh');
if (!$script || !is_executable($script)) {
    sse('sync.sh not found or not executable', 'error');
    exit;
}

$cmd    = 'sudo -n -u rlm ' . escapeshellarg($script) . ' 2>&1';
$handle = popen($cmd, 'r');
if (!$handle) {
    sse('Failed to start sync process', 'error');
    exit;
}

while (!feof($handle)) {
    $line = fgets($handle);
    if ($line !== false) {
        sse(rtrim($line));
    }
}

$rc = pclose($handle);
if ($rc === 0) {
    sse('', 'done');
} else {
    sse("Exited with code $rc", 'error');
}
