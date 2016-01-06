#!/bin/bash
#
# Test 8.  Import while selecting in another session.
#
. ../scripts/common.sh $1

TEST=test299
STATUSTEXT="299 Japanese Language Test:          "

$MYSQLCMD -e "set global infinidb_compression_type=1;"

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go299.sh
ret=$?
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT `cat $TEST/status.txt`" > $TEST.status
if [ $ret -ne 0 ]; then
	echo "$TEST Failure Details:" > $TEST.details
	echo "" >> $TEST.details
	cat $TEST/diff.txt >> $TEST.details
fi

cat $TEST.status
