#!/bin/bash
#
# Test 201.  Tests version buffer wrap around and version buffer overflow.  Expects 2 1GB files for the version buffer (the driving go.sh sets this).
#
. ../scripts/common.sh $1

TEST=test201
STATUSTEXT="201 Version Buffer Test:             "

$MYSQLCMD -e "set global infinidb_compression_type=1;"

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go201.sh
cd ..

#
# Populate the .status file.
#
diffs=`wc -l $TEST/diff.txt | awk '{print $1}'`
if [ $diffs -gt 0 ]; then
        echo "$STATUSTEXT Failed (check $TEST/diff.txt)" > $TEST.status
		ret=1
else
        echo "$STATUSTEXT Passed" > $TEST.status
		ret=0
fi

cat $TEST.status
exit $ret

