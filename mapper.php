#!/usr/bin/env php
<?php

// iterate through lines
while ($line = fgets(STDIN)) {
    // remove leading and trailing
    $line = ltrim($line);
    $line = rtrim($line);
    // split the line in words
    $words = preg_split('/\s/', $line, -1, PREG_SPLIT_NO_EMPTY);
    // iterate through words
    foreach ($words as $key) {
        // print word (key) to standard output
        // the output will be used in the
        // reduce (reducer.php) step
        // word (key) tab−delimited wordcount (1)
        printf("%s\t%d\n", $key, 1);
    }
}
