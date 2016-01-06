#!/bin/bash

#
# Script to reproduce problem in bug 4963.  The problem is exposed when there is an old block in the block cache that no longer should be read.
# The versioning information keeps us from reading the old block.  When the version buffer loops around, the versioning information related to
# the old block gets flushed and queries start thinking the old version is the suitable version, resulting in incorrect results.
#
# To stop the test early:
# touch stop.txt

#
# This script does the following setup.
# 1) Creates bug4963 table.
# 2) Imports one mil rows into bug4963.
# 3) Selects count(*) from bug4963 (this pulls the last partially filled block into the block cache).
# 4) Load data infiles another one mil rows into bug4963 (this adds rows to the last block in the table.
# 5) Creates a bug4963b table and loads it with 8 mil rows (all on PM1 so that all of the rows are on the same DBRoot).
#
# Then the test repeats the following for up to 40 times.  It kicks out if we get a bad count.
# 1) Updates the 8 mil rows in bug4963b.
# 2) Select count(*) from bug4963.

#
# Create source file.
#
rowsPerLoad=1000000
echo "Creating source file with $rowsPerLoad rows."
echo "" | awk -v rows=$rowsPerLoad '{for(i=1; i<=rows; i++)print i "0|"}' > /tmp/bug4963.tbl

#
# Setup the tables.
#

#
# 1) Create bug4963 table.
#
db=dmlc
sql="create database if not exists $db;
use $db;
drop table if exists bug4963;
create table bug4963(c1 bigint, c2 tinyint)engine=infinidb;"
echo "Creating $db.bug4963 table."
$MYSQLCMD -e "$sql;"
if [ $? -ne 0 ]; then
	exit 1
fi

loadSql="load data infile '/tmp/bug4963.tbl' into table bug4963 fields terminated by '|';"
selectSql="select count(*), if(count(*)%$rowsPerLoad=0, 'good', 'bad') from bug4963;"

#
# 2) Import rows into bug4963.
#
if $WINDOWS; then
	$CPIMPORTCMD $db bug4963 < /tmp/bug4963.tbl # Windows doesn't like the -m and -P parameter.
else
	$CPIMPORTCMD $db bug4963 /tmp/bug4963.tbl -m 1 -P 1
fi
if [ $? -ne 0 ]; then
	exit 1
fi

#
# 3) Select the rows just imported.
#
qResult=`$MYSQLCMD $db --skip-column-names -e "$selectSql"`
if [ $? -ne 0 ]; then
	exit 1
fi
echo $qResult

#
# 4) Load data infile another one mil rows into bug4963.
#
$MYSQLCMD $db -e "$loadSql"
if [ $? -ne 0 ]; then
	exit 1
fi

#
# 5) Select from bug4963 again.
#
qResult=`$MYSQLCMD $db --skip-column-names -e "$selectSql"`
if [ $? -ne 0 ]; then
	exit 1
fi
echo $qResult

#
# 6) Create bug4963b and load it with 8 mil rows all on PM1.
#
$MYSQLCMD $db -e "drop table if exists bug4963b; create table bug4963b(c1 bigint)engine=infinidb;"
if [ $? -ne 0 ]; then
	exit 1
fi

if $WINDOWS; then
	echo "" | awk '{for(i=1; i<=8*1000*1000; i++)print i}' | $CPIMPORTCMD $db bug4963b 
else
	echo "" | awk '{for(i=1; i<=8*1000*1000; i++)print i}' | $CPIMPORTCMD $db bug4963b -m 1 -P 1
fi

#
# Loop and repeat updates of bug4963b and selects against bug4963.
#
iterations=40
i=0
while [ $i -lt $iterations ]; do
	let i++;
	echo "Running update $i of $iterations at `date`."
	$MYSQLCMD $db -e "update bug4963b set c1=$i;"
	qResult=`$MYSQLCMD $db --skip-column-names -e "$selectSql"`
	if [ $? -ne 0 ]; then
		exit 1
	fi
	echo $qResult
	badCount=`echo $qResult | grep 'bad' | wc -l`
	if [ $badCount -gt 0 ]; then
		exit 1
	fi

        if [ -f stop.txt ]; then
                echo "Found stop.txt.  Stopping."
		rm -f stop.txt
                exit 0
        fi
done

$MYSQLCMD $db -e "drop table if exists bug4963; drop table if exists bug4963b;"
exit 0
