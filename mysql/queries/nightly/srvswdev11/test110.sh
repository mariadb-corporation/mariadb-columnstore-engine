#!/bin/bash
#
# Test 110.  Drop partition test.  
#
#		Imports rows, drops a partition, disables another partition, does some DML, then runs a few
#		validation queries.  Test110 is the same as test010 using a compressed table.

. ../scripts/common.sh $1

TEST=test110
STATUSTEXT="110 Drop Partition Test dmlc:        "

$MYSQLCMD -e "set global infinidb_compression_type=1;"


# Default the status to In Progress.
echo "$STATUSTEXT In Progress" > $TEST.status

rm -rf $TEST
cp -r test010 $TEST

cd $TEST
./go10.sh dmlc > $TEST.log 2>&1
cd ..

#
# Populate the .status file.
#
eval $(cat $TEST/*.log | tr '[:upper:]' '[:lower:]' | awk 'BEGIN{good=bad=error=0;} /good/ {good++;} /bad/ {bad++;} /error/ {error++} END{printf "good=%d\nbad=%d\nerror=%d\n", good, bad, error;}')

# If no good counts, fail it.
if [ $error -gt 0 ] || [ $good -eq 0 ] || [ $bad -gt 0 ]; then
        echo "$STATUSTEXT Failed (bad count=$bad, good count=$good, error count=$error)" > $TEST.status
		ret=1
else
        echo "$STATUSTEXT Passed ($good counts all matched!)" > $TEST.status
		ret=0
fi

echo ""
cat $TEST.status
exit $ret

