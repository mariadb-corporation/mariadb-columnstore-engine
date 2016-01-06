#!/bin/bash
#
# Test 297.  Miscellaneous tests.
#
. ../scripts/common.sh $1

TEST=test297
STATUSTEXT="297 Miscellaneous Tests:             "

$MYSQLCMD -e "set global infinidb_compression_type=1;"

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go297.sh
ret=$?
cd ..

#
# Populate the .status file.
#
if [ $ret -eq 0 ]; then
	echo "$STATUSTEXT Passed" > $TEST.status
else
	echo "$STATUSTEXT Failed (See $TEST.details)" > $TEST.status
	echo "$TEST Failure Details:" > $TEST.details
	echo "" >> $TEST.details
	cat $TEST/diff.txt >> $TEST.details
fi

cat $TEST.status
