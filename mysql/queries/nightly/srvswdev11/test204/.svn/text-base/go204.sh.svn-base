#!/bin/bash

source ../../scripts/mysqlHelpers.sh

DB=dmlc
ITERATIONS=200

$MYSQLCMD $DB < create.sql > create.sql.log 2>&1

for((i=1; i<=$ITERATIONS; i++))
do
	echo "Running iteration $i of $ITERATIONS at `date`."

        if [ -f stop.txt ]; then
                rm -f stop.txt
                echo "Found stop.txt.  Exiting script."
                exit
        fi

	let start=$i-1
	let start=$start*1000+1
	let stop=$start+999

	rm -f test204.tbl
	touch test204.tbl

	for((j=$start; j<=$stop; j++))
	do
		echo "$j|$j" >> test204.tbl
	done

	$MYSQLCMD $DB < query.sql > q1.sql.log 2>&1 &
	$MYSQLCMD $DB < query.sql > q2.sql.log 2>&1 &
	./load.sh &
	$MYSQLCMD $DB < query.sql > q3.sql.log 2>&1 &
	$MYSQLCMD $DB < query.sql > q4.sql.log 2>&1 &
	$MYSQLCMD $DB < query.sql > q5.sql.log 2>&1 &
	wait

	for j in $start $stop
	do
		count=`getCount $DB "select count(*) from test204 where c1=$j;"`	
		if [ $count -lt 1 ]; then
			echo "Failed (test204.c1 extent map incorrect)" 
			echo "Failed (test204.c1 extent map incorrect)" > status.txt
			exit 1
		else	
			echo "test204.c1 extent map is okay."
		fi
	
		count=`getCount $DB "select count(*) from test204 where c2=$j;"`	
		if [ $count -lt 1 ]; then
			echo "Failed (test204.c2 extent map incorrect)"
			echo "Failed (test204.c2 extent map incorrect)" > status.txt
			exit 1
		else	
			echo "test204.c2 extent map is okay."
		fi
	done

 	errs=`grep -i ERROR *.sql.log | wc -l`
	if [ $errs -gt 0 ]
	then
		echo "Failed (check test204/*log for errors)"
		echo "Failed (check test204/*log for errors)" > status.txt
	fi

done

echo "Passed ($ITERATIONS imports)" > status.txt
exit 0

