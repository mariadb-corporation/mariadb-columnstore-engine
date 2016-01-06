#!/bin/bash
#
# Test 109.  Do deletes while selecting counts in another session with compressed tables.
#
. ../scripts/common.sh $1

TEST=test109
STATUSTEXT="109 Count while deleting dmlc:       "

$MYSQLCMD -e "set global infinidb_compression_type=1;"

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

rm -rf test109
cp -r test009 test109

cd $TEST
./go9.sh dmlc
ret=$?
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt)" > $TEST.status

cat $TEST.status
exit $ret

