#!/bin/bash

#
# Runs parallel loads into customer tables.
#
# Note:
# Copy a tpch customer.tbl import file into this folder before running.
#

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

threads=20
iterations=10
db=walt
useImport=0

create() {
	tableName=$1
	sql="CREATE TABLE if not exists $tableName (
	  c_custkey int(11) DEFAULT NULL,
	  c_name varchar(25) DEFAULT NULL,
	  c_address varchar(40) DEFAULT NULL,
	  c_nationkey int(11) DEFAULT NULL,
	  c_phone char(15) DEFAULT NULL,
	  c_acctbal decimal(12,2) DEFAULT NULL,
	  c_mktsegment char(10) DEFAULT NULL,
	  c_comment varchar(117) DEFAULT NULL
	) ENGINE=InfiniDB; truncate table $tableName;"

	echo "`date` - Creating table $tableName."
	$MYSQLCMD $db -n -vvv -e "$sql" > $tableName.log 2>&1
	echo "`date` - Done creating table $tableName."
}

createTimes() {
	sql="drop table if exists times; create table times(iteration int, start datetime, stop datetime);"
	$MYSQLCMD $db -e "$sql"
}

load() {
	tableName=$1
	sql="load data infile '/tmp/$tableName.tbl' into table $tableName fields terminated by '|';"

	echo "`date` - Loading table $tableName."
	$MYSQLCMD $db -n -vvv -e "$sql" >> $tableName.log 2>&1
	if [ $? -ne 0 ]; then
		echo "`date` - Error loading table $tableName."
	else
		echo "`date` - Done loading table $tableName."
	fi
}

import() {
	tableName=$1

	echo "`date` - Importing table $tableName."
	/usr/local/Calpont/bin/cpimport $db $tableName /tmp/$tableName.tbl >> $tableName.log 2>&1
	if [ $? -ne 0 ]; then
		echo "`date` - Error importing table $tableName."
	else
		echo "`date` - Done importing table $tableName."
	fi
}

$MYSQLCMD -e "create database if not exists $db;"
createTimes

for((i=1; i<=$threads; i++)); do
	tableName="customer_$i"
	create $tableName &
done
wait

echo ""
for((i=1; i<=$threads; i++)); do
	tableName="customer_$i"
	echo "`date` - Creating file /tmp/$tableName.tbl"
	cp customer.tbl /tmp/$tableName.tbl 
done

echo ""
for((i=1; i<=$iterations; i++)); do
	echo ""
	echo "Running iteration $i of $iterations."
	$MYSQLCMD $db -e "insert into times values ($i, now(), null);"
	for((j=1; j<=$threads; j++)); do
		tableName="customer_$j"
		if [ $useImport -eq 1 ]; then
			import $tableName &
		else
			load $tableName &
		fi
	done
	wait
	$MYSQLCMD $db -e "update times set stop=now() where iteration=$i;"
done

echo ""
echo "Removing /tmp/customer*.tbl."
rm -f /tmp/customer*tbl

echo ""
echo "Run Times"
$MYSQLCMD $db --table -e "select iteration, start, timediff(stop, start) time from times;"

echo ""
echo "Validating table counts."
rows=`wc -l customer.tbl | awk '{print $1}'`
let totalRows=$rows*$iterations
goodCount=0
for((i=1; i<=$threads; i++)); do
	tableName="customer_$i"
	rows=`$MYSQLCMD $db --skip-column-names -e "select count(*) $tableNameCount from $tableName;"`
	if [ $rows -eq $totalRows ]; then
		let goodCount=$goodCount+1;
	else
		echo "Count for $tableName is $rows and should be $totalRows."
	fi
done
echo "$goodCount of $threads table counts match!"

echo ""
echo "All done."
