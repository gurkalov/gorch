<?php

//$output = shell_exec('clickhouse-client --query="select value from events order by value asc;"');
$output = shell_exec('clickhouse-client --query="select value from events where value >= \'1044500\' AND value < \'1045500\' order by value asc;"');
file_put_contents('a.txt', $output);

$array = explode(PHP_EOL, $output);
$uniq = array_unique($array);

print_r($uniq);
echo "Uniq:" . count($uniq);

$number = range(1044500, 1045500 - 1);

file_put_contents('b.txt', implode(PHP_EOL, $number));

echo "Count: " . count($number);
$diff = array_diff($number, $array);
print_r($diff);
echo "Count: " . count($array);
$diff = array_diff($array, $number);
print_r($diff);

$result = array_intersect($array, $number);
print_r(count($result));

$result = array_intersect_key($array, $number);
print_r(count($result));
