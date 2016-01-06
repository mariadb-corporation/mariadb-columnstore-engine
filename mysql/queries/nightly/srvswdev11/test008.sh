#!/bin/bash
#
# Test 8.  Import while selecting in another session.
#
. ../scripts/common.sh $1

TEST=test008
STATUSTEXT="008 Count while importing:           "

$MYSQLCMD -e "set global infinidb_compression_type=0;"


# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go8.sh
ret=$?
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt)" > $TEST.status

cat $TEST.status
exit $ret

