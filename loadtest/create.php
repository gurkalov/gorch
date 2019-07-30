<?php
$filename = 'ammo.txt';
unlink($filename);
$file = fopen($filename, 'w');
fwrite($file, "[Host: example.org]" . PHP_EOL);
fwrite($file, "[Connection: close]" . PHP_EOL);
fwrite($file, "[User-Agent: Tank]" . PHP_EOL);
fwrite($file, "[Content-Type: application/json]" . PHP_EOL);

$base = 1000000;
for ($i = 0; $i <= 1000000; $i++)
{
    fwrite($file, '51 /add' . PHP_EOL);
    fwrite($file, '{"user_id": 1, "path": "test", "value": "'. ($base + $i) .'"}' . PHP_EOL);
}
fclose($file);
