#!/bin/bash
#
# Test 011.  Test cpimport features.
#
. ../scripts/common.sh $1

TEST=test011
STATUSTEXT="011 cpimport Features Test:          "

echo "$STATUSTEXT In Progress" > $TEST.status

$MYSQLCMD -e "set global infinidb_compression_type=0;"

#
# Run the test with results going to .log file.
#
cd $TEST
./go011.sh > ../$TEST.log 2>&1
cd ..

# 
# Populate the .status file.
#
grep Failed $TEST.log > failed.txt
lines=`cat failed.txt | wc -l`
if [ $lines -gt 0 ]
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
