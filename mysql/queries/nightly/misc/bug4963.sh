#!/bin/bash

#
# Script to reproduce problem in bug 4193.  The problem is exposed when there is an old block in the block cache that no longer should be read.
# The versioning information keeps us from reading the old block.  When the version buffer loops around, the versioning information related to
# the old block gets flushed and queries start thinking the old version is the suitable version, resulting in incorrect results.
#
# To stop the test early:
# touch stop.txt

#
# This script does the following setup.
# 1) Creates t1 table.
# 2) Imports one mil rows into t1.
# 3) Selects count(*) from t1 (this pulls the last partially filled block into the block cache).
# 4) Load data infiles another one mil rows into t1 (this adds rows to the last block in the table.
# 5) Creates a t2 table and loads it with 8 mil rows (all on PM1 so that all of the rows are on the same DBRoot).
#
# Then the test repeats the following for up to 200 times.  It kicks out if we get a bad count.
# 1) Updates the 8 mil rows in t2.
# 2) Select count(*) from t1.


if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

#
# Create source file.
#
rowsPerLoad=1000000
echo "Creating source file with $rowsPerLoad rows."
echo "" | awk -v rows=$rowsPerLoad '{for(i=1; i<=rows; i++)print i "0|"}' > /tmp/t1.tbl

#
# Setup the tables.
#

#
# 1) Create t1 table.
#
db=walt
sql="create database if not exists $db;
use $db;
drop table if exists t1;
create table t1(c1 bigint, c2 tinyint)engine=infinidb;"
echo "Creating $db.t1 table."
$MYSQLCMD -e "$sql;"

loadSql="load data infile '/tmp/t1.tbl' into table t1 fields terminated by '|';"
selectSql="select count(*), if(count(*)%$rowsPerLoad=0, 'good', 'bad') from t1;"

#
# 2) Import rows into t1.
#
/usr/local/Calpont/bin/cpimport $db t1 /tmp/t1.tbl -m 1 -P 1

#
# 3) Select the rows just imported.
#
qResult=`$MYSQLCMD $db --skip-column-names -e "$selectSql"`
echo $qResult

#
# 4) Load data infile another one mil rows into t1.
#
$MYSQLCMD $db -e "$loadSql"

#
# 5) Select from t1 again.
#
qResult=`$MYSQLCMD $db --skip-column-names -e "$selectSql"`
echo $qResult

#
# 6) Create t2 and load it with 8 mil rows all on PM1.
#
$MYSQLCMD $db -e "drop table if exists t2; create table t2(c1 bigint)engine=infinidb;"
echo "" | awk '{for(i=1; i<=8*1000*1000; i++)print i}' | /usr/local/Calpont/bin/cpimport $db t2 -m 1 -P 1

#
# Loop and repeat updates of t2 and selects against t1.
#
iterations=200
i=0
while [ $i -lt $iterations ]; do
	let i++;
	echo "Running update $i of $iterations at `date`."
	$MYSQLCMD $db -e "update t2 set c1=$i;"
	qResult=`$MYSQLCMD $db --skip-column-names -e "$selectSql"`
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

exit 0
