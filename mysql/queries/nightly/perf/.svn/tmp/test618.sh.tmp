#!/bin/sh
#
# Test 618.  Validate the ssb1992c data.
#
TEST=test618
STATUSTEXT="618 Ssb1992Q1c Data Validation         : "
DB=ssb1992Q1c

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

#
# Run validation queries.  Use the test615 folder for the dimension table validation.
#
../../queryTester -q $TEST -u $DB -t lineorderDataValidation -r Tr -g >> $TEST.log 2>&1

#
# Populate the .status file.
#
errorCount=`grep -i error $TEST.log | wc -l`
grep failed $TEST.log > failed.txt
failedCount=`cat failed.txt | wc -l`

if [ $errorCount -gt 0 ]
then
        echo "$STATUSTEXT Failed (Check $TEST.log for errors)" > $TEST.status
elif [ $failedCount -gt 0 ]
then
        echo "$STATUSTEXT Failed (Data validation failed.  See details below.)" > $TEST.status
else
        echo "$STATUSTEXT Passed" > $TEST.status
fi

# List the individual sql validation scripts that failed in the .details file.
if [ $failedCount -gt 0 ]
then
        echo "$TEST validation scripts that failed:" > $TEST.details
        cat failed.txt >> $TEST.details
fi

cat $TEST.status

