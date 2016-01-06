#!/bin/bash
#
# Test 202.  Uses queryTester to run the wide folder tests testing against tables with many columns and tables with wide string 
#            columns.
#
. ../scripts/common.sh $1

TEST=test202
STATUSTEXT="202 Wide Table Tests:                " 

echo "$STATUSTEXT In Progress" > $TEST.status

$MYSQLCMD -e "set global infinidb_compression_type=1;"

cd $TEST
./go202.sh > ../$TEST.log
cd ..

#
# Populate the .status file.
#
grep failed $TEST.log > failed.txt
lines=`cat failed.txt | wc -l`
if [ $lines -eq 0 ]
then
        echo "$STATUSTEXT Passed" > $TEST.status
		ret=0
else
        echo "$STATUSTEXT Failed" > $TEST.status
		ret=1
fi

# 
# Populate the .details file.
#

# List individual scripts that failed.
grep failed $TEST.log > failed.txt
lines=`cat failed.txt | wc -l`
if [ $lines -gt 0 ]
then
        echo "Failed Wide Table Tests:" > $TEST.details 
        cat failed.txt >> $TEST.details
fi
rm -f failed.txt

cat $TEST.status
exit $ret

