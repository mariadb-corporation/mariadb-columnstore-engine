#!/bin/bash
#
# Test 700.  Create tpch100 tables.
#
TEST=test700
STATUSTEXT="700 Create tpch100 tables          : "

SCHEMA=tpch100

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

#
# Rebuild the databases with compression off.
#
$MYSQLCMD -e "set global infinidb_compression_type=0;"
$MYSQLCMD -e "create database if not exists $SCHEMA;"
$MYSQLCMD $SCHEMA < ../../../scripts/create_tpch_4byte.sql > $TEST.log 2>&1
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

