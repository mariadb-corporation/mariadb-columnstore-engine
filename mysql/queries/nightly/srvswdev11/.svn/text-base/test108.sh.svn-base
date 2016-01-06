#!/bin/bash
#
# Test 108.  Import while selecting counts in another session against compressed table.
#
. ../scripts/common.sh $1

TEST=test108
STATUSTEXT="108 Count while importing dmlc:      "

$MYSQLCMD -e "set global infinidb_compression_type=1;"

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

rm -rf test108
cp -r test008 test108

cd $TEST
./go8.sh dmlc
ret=$?
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt)" > $TEST.status

cat $TEST.status
exit $ret

