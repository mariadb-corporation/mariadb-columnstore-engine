#!/bin/bash
#
# Test 813.  Ssb100c data validation.
#
TEST=test813
STATUSTEXT="813 Data validation ssb100c        : " 
SCHEMA=ssb100c

source ../scripts/extentMapValidation.sh

# Clear the min and max for the ssb100c schema extent map entries.
clearExtentMapMinMax $SCHEMA

cd ../..
time ./queryTester -q ssb100 -u $SCHEMA -r Tr >> nightly/srvalpha2/$TEST.log 2>&1
cd nightly/srvalpha2

#
# Populate the .status file.
#
errorCount=`grep -i error $TEST.log | wc -l`
grep failed $TEST.log > failed.txt
failedCount=`cat failed.txt | wc -l`

if [ $errorCount -gt 0 ]; then
        echo "$STATUSTEXT Failed (Check $TEST.log for errors)" > $TEST.status
elif [ $failedCount -gt 0 ]; then
        echo "$STATUSTEXT Failed (Data validation failed.  See details below.)" > $TEST.status
else
        echo "$STATUSTEXT Passed" > $TEST.status
fi

#
# List the individual sql validation scripts that failed in the .details file.
#
if [ $failedCount -gt 0 ]; then
        echo "$TEST validation scripts that failed:" > $TEST.details
        cat failed.txt >> $TEST.details
fi

cat $TEST.status
