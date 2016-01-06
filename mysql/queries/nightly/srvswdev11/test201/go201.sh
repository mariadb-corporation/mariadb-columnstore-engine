#!/bin/bash

#
# Version buffer wrap around and overflow test.  Expects the version buffer to be 2 files at 1 GB each.  
#   1) Populates a table with 150 million rows.
#   2) Runs multiple update statements against the table

DB=dmlc

#
# 150 mil rows is enough to overflow the VersionBuffer by updating the two bigints across all blocks when configured with 2 1GB files.
#
ROWS=150000000

rm -f diff*
echo "Test still running." > diff.txt

#
# Create and populate the $DB.test201.  Table gets populated w/ $ROWS rows with values 1 through $ROWS.
#
$MYSQLCMD -e "create database if not exists $DB;"
echo "Creating and populating $DB.test201 table."
$MYSQLCMD $DB -vvv -e "drop table if exists test201; create table test201(c1 int, c2 bigint, c3 bigint)engine=infinidb;" > create.sql.log 2>&1

#
# Get the DBRootCount and adjust the number of rows / adjust the size of the updates accordingly.
#
dbRootCount=`../../scripts/getConfig.sh SystemConfig DBRootCount`
if [ $dbRootCount -eq 1 ]; then
	ROWS=75000000
elif [ $dbRootCount -eq 2 ]; then
	ROWS=150000000
elif [ $dbRootCount -eq 4 ]; then
	ROWS=300000000
elif [ $dbRootCount -eq 6 ]; then
	ROWS=450000000
else
	echo "Script does not yet support DBRootCount of $dbRootCount." > diff.txt
	exit
fi
cp test201.sql.$dbRootCount test201.sql
cp test201.sql.$dbRootCount.ref.log test201.sql.ref.log


echo "dummy" | awk -v rows=$ROWS '{for(i=1;i<=rows;i++)print i "|" i "|" i}' | $CPIMPORTCMD $DB test201 > import.log

#
# Run the test201.sql script.
#
echo "Running Version Buffer overflow and wrap around test.  Takes a while."
$MYSQLCMD $DB -f -n -vvv < test201.sql > test201.sql.log 2>&1

#
# Run a diff report.
#
egrep -v "affected|sec" test201.sql.log > diff.log
egrep -v "affected|sec" test201.sql.ref.log > diff.ref.log
diff -b diff.ref.log diff.log > diff.txt 2>&1

