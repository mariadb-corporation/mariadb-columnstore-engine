#!/bin/bash
#
# Test 112.  Test the functionalities of varbinary
#
# NOTE:  Runs as test112 as a repeat run with compressed tables.

. ../scripts/common.sh $1

TEST=test112
STATUSTEXT="112 Varbinary Test:                  "

echo "$STATUSTEXT In Progress" > $TEST.status

$MYSQLCMD -e "set global infinidb_compression_type=1;"

rm -rf test112
cp -r test012 test112

cd $TEST
./go12.sh dmlc > ../$TEST.log 2>&1
ret=$?
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt)" > $TEST.status

cat $TEST.status
exit $ret

