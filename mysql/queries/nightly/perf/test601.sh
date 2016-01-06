#!/bin/sh
#
# Test 601.  Rebuild the ssb100c tables.
#
TEST=test601
STATUSTEXT="601 Drop/Create Ssb100c Tables         : "


if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

DB=ssb100c
DB2=ssb100cM2
DB3=ssb100cM3
DB4=ssb1992c
DB5=ssb1992Q1c
echo "$STATUSTEXT In Progress" > $TEST.status

#
# Rebuild the ssb100c database.
#
$MYSQLCMD -e "create database if not exists $DB;" -vvv > $TEST.log 2>&1
$MYSQLCMD $DB -vvv < ../../../scripts/create_ssb_schema.sql >> $TEST.log 2>&1

#
# Rebuild the ssb100cM2 database for the table imported via mode 2.
#
$MYSQLCMD -e "create database if not exists $DB2;" -vvv >> $TEST.log 2>&1
$MYSQLCMD $DB2 -vvv < ../../../scripts/create_ssb_schema.sql >> $TEST.log 2>&1

#
# Rebuild the ssb100cM3 database for the table imported via mode 2.
#
$MYSQLCMD -e "create database if not exists $DB3;" -vvv >> $TEST.log 2>&1
$MYSQLCMD $DB3 -vvv < ../../../scripts/create_ssb_schema.sql >> $TEST.log 2>&1

#
# Rebuild the ssb1992c database for the select into import.
#
$MYSQLCMD -e "create database if not exists $DB4;" -vvv >> $TEST.log 2>&1
$MYSQLCMD $DB4 -vvv < ../../../scripts/create_ssb_schema.sql >> $TEST.log 2>&1

#
# Rebuild the ssb1992c database for the select into import.
#
$MYSQLCMD -e "create database if not exists $DB5;" -vvv >> $TEST.log 2>&1
$MYSQLCMD $DB5 -vvv < ../../../scripts/create_ssb_schema.sql >> $TEST.log 2>&1

#
# Populate the .status file.
#
errorCount=`grep -i error $TEST.log | wc -l`

if [ $errorCount -gt 0 ]
then
        echo "$STATUSTEXT Failed (Check $TEST.log for errors)" > $TEST.status
else
        echo "$STATUSTEXT Passed" > $TEST.status
fi

cat $TEST.status

