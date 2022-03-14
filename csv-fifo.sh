#!/bin/bash
set -e

DUCK_DB="../duckdb/build/release/duckdb"
PIPE=/tmp/testpipe.csv

rm -f $PIPE
mkfifo $PIPE

if [ -t 0 ]; then
    $DUCK_DB -s "select sum(column1) from read_csv_auto('$PIPE')" &
    echo -e "1,1\n2,2\n3,3" > $PIPE
else
    $DUCK_DB -s "select count(*) from read_csv_auto('$PIPE')" &
    cat - > $PIPE
fi

rm -f $PIPE

wait