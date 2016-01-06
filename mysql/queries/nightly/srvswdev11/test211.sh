#!/bin/bash
#
# Test 211.  Tests concurrent transactions.
#
# Three optional parameters:
# 1) # of tables / concurrent sessions
# 2) Rows for table used by first thread, second thread gets rows * 2, third thread gets rows * 3, etc.
# 3) Time to run in seconds.
#
if [ -z "$INSTALLDIR" ]; then
		echo "You must run '. ../scripts/common.sh' before running this script."
		exit
fi

TEST=test211
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
