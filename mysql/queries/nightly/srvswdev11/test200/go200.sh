#!/bin/bash

source ../../scripts/mysqlHelpers.sh

DB=dmlc
rows=150000000

rm -f memLimits.sql.log create.sql.log import.log
echo "Test still running." > diff.txt

#
# Create and populate the $DB.test200 table if necessary.  Table gets populated w/ 150 mil rows values 1 through 150 mil.
#
$MYSQLCMD -e "create database if not exists $DB; use $DB; create table if not exists test200(c1 int);"
count=`getCount $DB "select count(*) from test200;"`
if [ $count -ne $rows ]
then
	echo "Creating and populating $DB.test200 table."
	$MYSQLCMD $DB -vvv -e "drop table if exists test200; create table test200(c1 int, c2 bigint, c3 varchar(1000))engine=infinidb; drop table if exists test200b; create table if not exists test200b(c1 int)engine=infinidb;" > create.sql.log 2>&1
	echo "dummy" | awk -v rows=$rows '{for(i=1;i<=rows;i++)print i "|" i "|" i}' | $CPIMPORTCMD $DB test200 > import.log
	echo "dummy" | awk -v rows=$rows '{for(i=1;i<=rows;i++)print i}' | $CPIMPORTCMD $DB test200b >> import.log
fi

#
# Run the memLimits.sql script.
#
echo "Running memory limit selects.  Takes a while."
if $WINDOWS; then
	$MYSQLCMD $DB -f -n < memLimits.sql.win > memLimits.sql.win.log 2>&1
	diff -b memLimits.sql.win.ref.log memLimits.sql.win.log > diff.txt 2>&1
else
	$MYSQLCMD $DB -f -n < memLimits.sql > memLimits.sql.log 2>&1
	diff -b memLimits.sql.ref.log memLimits.sql.log > diff.txt 2>&1
fi
