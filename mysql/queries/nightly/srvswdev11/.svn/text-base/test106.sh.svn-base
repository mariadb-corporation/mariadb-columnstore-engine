#!/bin/bash
#
# Test 106.  Load data infile in one session while selecting counts in another with compression on.  Uses test006.
#
. ../scripts/common.sh $1

TEST=test106
STATUSTEXT="106 Count while loading dmlc:        "

$MYSQLCMD -e "set global infinidb_compression_type=1;"

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

rm -rf test106
cp -r test006 test106

cd $TEST
./go6.sh dmlc
ret=$?
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt)" > $TEST.status

cat $TEST.status
exit $ret

