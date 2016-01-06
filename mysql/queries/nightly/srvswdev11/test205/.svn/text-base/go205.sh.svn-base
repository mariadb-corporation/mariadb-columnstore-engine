#!/bin/bash

i=0
db=dmlc

doImports() {
	table=$1
	imports=$2
	rows=$3

	sql="
		create database if not exists $db;
		use $db;
		drop table if exists $table;
		create table if not exists $table (
			batch int,
			c1    int,
			c2    varchar(50),
			c3    varchar(100),
			c4    varchar(100)
		)engine=infinidb;"
	
	$MYSQLCMD -vvv -e "$sql" > $table.log 2>&1
	sleep 5

	for((i=1; i<=$imports; i++)); do
        	if [ -f stop.txt ]; then
        	        echo "Found stop.txt.  Exiting script for table $table."
			break
        	fi
		echo "Running import $i of $imports for table $table at `date`."
		echo "" | awk -v batch=$i -v rows=$rows '{for(i=1; i<=rows; i++)printf "%d|%d|%012d|%012d|%012d|\n", batch, i, i, i, i;}' |
			/usr/local/Calpont/bin/cpimport $db $table >> $table.log 2>&1
	        rc=$?
	        if [ $rc -ne 0 ]; then
			echo "Error on import $i for table $table."
			break;
	        fi
	done
}

#
# Main.  Run multiple doImports in parallel.
#
tables=4
imports=500
rows=300000

for((j=1; j<=$tables; j++)); do
	doImports test205_$j $imports $rows &
done

wait

echo "All done."
