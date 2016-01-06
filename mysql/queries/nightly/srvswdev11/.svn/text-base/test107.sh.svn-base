#!/bin/bash
#
# Test 107.  Updates in one session while selecting counts in another session w/ compression turned on.  Uses test007.
#
. ../scripts/common.sh $1

TEST=test107
STATUSTEXT="107 Count while updating dmlc:       "

$MYSQLCMD -e "set global infinidb_compression_type=1;"

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

rm -rf test107
cp -r test007 test107

cd $TEST
./go7.sh dmlc
ret=$?
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt)" > $TEST.status

cat $TEST.status
exit $ret

