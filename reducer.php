#!/usr/bin/env php
<?php

$last_key = NULL;
$running_total = 0;

// iterate through lines
while ($line = fgets(STDIN)) {
    // remove leading and trailing
    $line = ltrim($line);
    $line = rtrim($line);

    if (strlen($line) == 0)
        continue;

    // split line into key and count
    list($key, $count) = explode("\t", $line);

    // this if-statement works because
    // hadoop sorts the mapper output by its keys
    // before sending it to the reducer
    // if the last key retrieved is the same
    // as the current key that has been received.
    if ($last_key == $key) {
        $running_total += $count;
    } else {
        if ($last_key != NULL) {
            // output previous key and its running total
            printf("%s\t%d\n", $last_key, $running_total);
        }
        // reset last key and running total
        // by assigning the new key and its value
        $last_key = $key;
        $running_total = $count;
    }
}

if ($last_key != NULL) {
    printf("%s\t%d\n", $last_key, $running_total);
}
