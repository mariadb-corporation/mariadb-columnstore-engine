#!/bin/bash
#
# Test 210.  Tests scenario from bug 4963 where a transaction the VSS wraps around and flushes information on a block that has an old version in the block cache.
#
. ../scripts/common.sh $1

TEST=test210
STATUSTEXT="210 VSS Flush Block in Block Cache:  "

# Default the status to In Progress.
echo "$STATUSTEXT In Progress" > $TEST.status

$TEST/go210.sh > $TEST.log 2>&1
ret=$?

#
# Populate the .status file.
#
if [ $ret -eq 0 ]; then
	echo "$STATUSTEXT Passed" > $TEST.status
else
        echo "$STATUSTEXT Failed (check $TEST.log)" > $TEST.status
fi

cat $TEST.status
exit $ret
