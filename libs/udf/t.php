<?php

$udf = FFI::cdef("double ArraySum(double numbers[], int size);", __DIR__ . '/libudf.so'
);

$arr = FFI::new('double[100000000]');

for ($i = 0; $i < 100000000; ++$i) {
    $arr[$i] = $i;
}

$start = microtime(true);
echo 'Calculate sum of 10 billion doubles using Go', PHP_EOL;
var_dump($udf->ArraySum($arr, 100000000));
echo 'Usage time:', (microtime(true) - $start) * 1000, 'ms', PHP_EOL;

$arr = [];
for ($i = 0; $i < 100000000; ++$i) {
    $arr[$i] = $i;
}
$start = microtime(true);
echo 'Calculate sum of 10 billion doubles using PHP', PHP_EOL;
var_dump(array_sum($arr));
echo 'Usage time:', (microtime(true) - $start) * 1000, 'ms', PHP_EOL;
