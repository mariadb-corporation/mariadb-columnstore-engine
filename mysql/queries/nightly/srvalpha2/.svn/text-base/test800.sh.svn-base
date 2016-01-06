#!/bin/bash
#
# Test 800.  Create ssb100 tables.
#

. ../scripts/common.sh $1

TEST=test800
STATUSTEXT="800 Create ssb100 tables           : "
SCHEMA=ssb100

#
# Rebuild the databases with compression off.
#
$MYSQLCMD -e "set global infinidb_compression_type=0;"
$MYSQLCMD -e "create database if not exists $SCHEMA;"
$MYSQLCMD $SCHEMA < ../../../scripts/create_ssb_schema.sql > $TEST.log 2>&1
$MYSQLCMD -e "set global infinidb_compression_type=1;"

#
# Populate the .status file.
#
errorCount=`grep -i error $TEST.log | wc -l`
grep failed $TEST.log > failed.txt
failedCount=`cat failed.txt | wc -l`
rm -f failed.txt

if [ $errorCount -gt 0 ]
then
        echo "$STATUSTEXT Failed (Check $TEST.log for errors)" > $TEST.status
else
        echo "$STATUSTEXT Passed" > $TEST.status
fi

cat $TEST.status

