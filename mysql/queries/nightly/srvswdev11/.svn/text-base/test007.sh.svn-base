#!/bin/bash
#
# Test 7.  Updates in one session while selecting counts in another session w/ compression turned off.
#
. ../scripts/common.sh $1

TEST=test007
STATUSTEXT="007 Count while updating:            "

$MYSQLCMD -e "set global infinidb_compression_type=0;"

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go7.sh
ret=$?
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt)" > $TEST.status

cat $TEST.status
exit $ret

