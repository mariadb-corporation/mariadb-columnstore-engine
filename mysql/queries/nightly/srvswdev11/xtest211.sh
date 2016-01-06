#!/bin/bash
#
# Test 211.  Tests concurrent updates.
#
# Three optional parameters:
# 1) # of tables / concurrent sessions
# 2) Rows per table.
# 3) Time to run in seconds.
#
TEST=xtest211
STATUSTEXT="211 Concurrent Transactions Test:    "

# Default the status to In Progress.
echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go211.sh $1 $2 $3
ret=$?
cd ..

#
# Populate the .status file.
#
if [ $ret -eq 0 ]; then
	echo "$STATUSTEXT Passed `cat $TEST/status.txt`" > $TEST.status
else
        echo "$STATUSTEXT Failed `cat $TEST/status.txt`" > $TEST.status
fi

cat $TEST.status
exit $ret
