#!/bin/bash
#
# Test 102.  Concurrency test.  Runs the group queries against tpch1c in 32 concurrent sessions and validates that all outputs match.  This is
#            a repeat of test002.sh against compressed tables.
#
# NOTE:  Also called as test102 which is a repeat run w/ compressed tables.

. ../scripts/common.sh $1

TEST=test102
STATUSTEXT="102 Concurrency Test tpch1c:         "

echo "$STATUSTEXT In Progress" > $TEST.status

SESSIONS=32
DB=tpch1c

if $WINDOWS; then
    # For some reason, windows reports as Running but still isn't functional.
    sleep 180
fi
$MYSQLCMD -e "set global infinidb_compression_type=1;"

#
# Copy test002 directory to test102.
#
rm -rf test102
cp -r test002 test102

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

