#!/bin/bash
#
# Runs the cpimport bulkload features tests
#
# To add a new test.
# 1) Add create table statement to create_tables.sql.
# 2) Add call to cpimport to rebuild.sh.
# 3) Add a .sql file for the data validation to the sql directory and a corresponding .sql.log.ref with the correct results.
# 4) Optionally add an import file to the tables directory.

DB=bulkload_features
export DB

rm -f sql/*.sql.log
rm -f sql/*.diff
./rebuild.sh

for i in sql/*.sql; do
	$MYSQLCMD $DB < $i > $i.log 2>&1
done

for i in sql/*.sql 
do

	REFLOG=$i.log.ref
	if $WINDOWS; then
	    if [ -f $i.log.win.ref ]; then
			REFLOG=$i.log.win.ref
		fi
	fi
	if [ -e $i.log ] && [ -e $REFLOG ]
	then
		diff -b $i.log $REFLOG > $i.diff
		lines=`cat $i.diff | wc -l`
		if [ $lines -eq 0 ]
		then
			echo "Success for $i test."
		else
			echo "tested $i.log against $REFLOG"
			echo "Failed.  Check $i.diff for differences."
		fi
	else
		echo "Looked for $i.log against $REFLOG"
 		echo "Failed.  $i missing or $i.log.ref missing."
	fi
done
