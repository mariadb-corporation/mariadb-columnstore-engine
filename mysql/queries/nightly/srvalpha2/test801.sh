#!/bin/bash
#
# Test 801.  Import ssb100.
#
TEST=test801
STATUSTEXT="801 Import ssb100                  : " 

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

./ssb100_import_test.sh ssb100 > $TEST.log

status=`tail -1 $TEST.log`

#
# Populate the .status file.
#
errorCount=`grep -i error $TEST.log | wc -l`

if [ $errorCount -gt 0 ]
then
        echo "$STATUSTEXT Failed (Check $TEST.log for errors)" > $TEST.status
else
        echo "$STATUSTEXT Passed $status" > $TEST.status
fi

cat $TEST.status
