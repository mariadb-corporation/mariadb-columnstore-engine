#!/bin/bash

source ../../scripts/mysqlHelpers.sh

DB=dmlc
ITERATIONS=200

cp test203.tbl /tmp/test203.tbl

$MYSQLCMD $DB < create.sql > create.sql.log 2>&1

for((i=1; i<=$ITERATIONS; i++))
do
	echo "Running iteration $i of $ITERATIONS at `date`."

        if [ -f stop.txt ]; then
                rm -f stop.txt
                echo "Found stop.txt.  Exiting script."
                exit
        fi

	$MYSQLCMD $DB < query.sql > q1.sql.log 2>&1 &
	$MYSQLCMD $DB < query.sql > q2.sql.log 2>&1 &
	$MYSQLCMD $DB < dml.sql > dml.sql.log 2>&1 &
	$MYSQLCMD $DB < query.sql > q3.sql.log 2>&1 &
	$MYSQLCMD $DB < query.sql > q4.sql.log 2>&1 &
	$MYSQLCMD $DB < query.sql > q5.sql.log 2>&1 &
	wait

	count=`getCount $DB "select count(*) from test203 where c1=200;"`	
	if [ $count -lt 1 ]; then
		echo "Failed (test203.c1 extent map incorrect)" 
		echo "Failed (test203.c1 extent map incorrect)" > status.txt
		exit 1
	else	
		echo "test203.c1 extent map is okay."
	fi
	
	count=`getCount $DB "select count(*) from test203 where c2=200;"`	
	if [ $count -lt 1 ]; then
		echo "Failed (test203.c2 extent map incorrect)"
		echo "Failed (test203.c2 extent map incorrect)" > status.txt
		exit 1
	else	
		echo "test203.c2 extent map is okay."
	fi

 	errs=`grep -i ERROR *.sql.log | wc -l`
	if [ $errs -gt 0 ]
	then
		echo "Failed (check test203/*log for errors)"
		echo "Failed (check test203/*log for errors)" > status.txt
	fi

done

echo "Passed ($ITERATIONS loads)" > status.txt
exit 0

