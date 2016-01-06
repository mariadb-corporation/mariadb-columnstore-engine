#!/bin/bash
#
# Test 112.  Test the functionalities of varbinary
#
# NOTE:  Runs as test112 as a repeat run with compressed tables.
. ../scripts/common.sh $1

TEST=test012
STATUSTEXT="012 Varbinary Test:                  "

echo "$STATUSTEXT In Progress" > $TEST.status

$MYSQLCMD -e "set global infinidb_compression_type=0;"

cd $TEST
./go12.sh > ../$TEST.log 2>&1
ret=$?
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt)" > $TEST.status

cat $TEST.status
exit $ret

