#!/bin/bash
#
# Test 215.  Tests concurrent transactions with each thread running a long continuous session.
#
TEST=test215
STATUSTEXT="215 Concurrent Transactions Test:    "

# Default the status to In Progress.
echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go215.sh 
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
