#!/bin/bash
#
# Test 2.  Concurrency test.  Runs the group queries against tpch1 in 32 concurrent sessions and validates that all outputs match.
#
# NOTE:  Also called as test102 which is a repeat run w/ compressed tables.
. ../scripts/common.sh $1

TEST=test002
STATUSTEXT="002 Concurrency Test:                "
SESSIONS=32
DB=tpch1

echo "$STATUSTEXT In Progress" > $TEST.status

$MYSQLCMD -e "set global infinidb_compression_type=0;"


#
# Run the concurrency test with results going to .log file.
#
cd $TEST
./go2.sh $SESSIONS $DB > ../$TEST.log
cd ..

# 
# Populate the .status file.
#
grep Success $TEST.log > failed.txt
lines=`cat failed.txt | wc -l`
if [ $lines -eq 0 ]
then
        echo "$STATUSTEXT Failed (check /root/genii/mysql/queries/nightly/srvswdev11/$TEST.log)" > $TEST.status
		ret=1
else
        echo "$STATUSTEXT Passed" > $TEST.status
		ret=0
fi
rm -f failed.txt

cat $TEST.status
exit $ret

