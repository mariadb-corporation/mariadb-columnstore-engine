#!/bin/bash
#
# Test 6.  Load data infile in one session while selecting counts in another with compression off.
#
# NOTE:  Runs as test106 as a repeat run with compressed tables.
TEST=test006
STATUSTEXT="006 Count while loading:             "

. ../scripts/common.sh $1

$MYSQLCMD -e "set global infinidb_compression_type=0;"

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go6.sh

ret=$?

cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt)" > $TEST.status

cat $TEST.status

exit $ret

