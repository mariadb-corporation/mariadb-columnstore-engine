#!/bin/sh
DB=`../getDatabaseName.sh ssb100c`

ITERATIONS=10

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

#------------------------------------------------------------------------------------
# Run the fast query in a loop until the stop.txt file is present or this script is
# no longer running.
#------------------------------------------------------------------------------------
runFastQuery()
{
	while [ ! -f stop.txt ]
	do
	        lines=`ps | grep go504 | wc -l`
	        if [ $lines -eq 0 ]
	        then
			exit
	        fi
	        $MYSQLCMD $DB -vvv -n < veryFast.sql >> veryFast.sql.log 2>&1
		
	done
}


rm -f *.sql.log
rm -f stop.txt
runFastQuery &
for((i=1; i<=$ITERATIONS; i++))
do
        echo "Running slow query pass $i of $ITERATIONS."
        $MYSQLCMD $DB -vvv -n < slow.sql >> slow.sql.log
	if [ -f stop.txt ]
	then
		echo "Found stop.txt.  Stopping."
		rm -f stop.txt
		exit
	fi	
done
echo "Waiting for fast script to finish."
touch stop.txt
wait
rm -f stop.txt

#
# Report results.
#
fast=`grep sec veryFast.sql.log | wc -l`
slow=`grep sec slow.sql.log | wc -l`
errs=`grep -i error *.sql.log | wc -l`

if [ $errs -gt 0 ];then
        echo "Failed (veryFastQueries=$fast, slowQueries=$slow, errors=$errs)" > status.txt
else
        echo "Passed (veryFastQueries=$fast, slowQueries=$slow)" > status.txt
fi
