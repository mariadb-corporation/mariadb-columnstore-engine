#!/bin/bash

source ../../scripts/mysqlHelpers.sh

DB=dml
LOADS=50
ROWSPERLOAD=75000

rm -f logs/*

if [ $# -eq 1 ]; then
	DB=$1
fi

$MYSQLCMD -e "create database if not exists $DB;"
echo "Dropping old / creating new table."
$MYSQLCMD $DB -vvv < create.sql > logs/create.sql.log

#
# Loop and load.
#
rm -f import.log
rm -f stop.txt

for((i=1; i <= $LOADS; i++))
do
        # Build the next import file.
        echo "Running import $i of $LOADS."
	dt=`date '+%Y-%m-%d %H:%M:%S'`
        echo "dummy" | awk -v batch=$i -v dt="$dt" -v rows=$ROWSPERLOAD '{ for (i=1; i<=rows; i++){print batch "|" dt "|" i "|" i "|" i "|" i "|" i}; }' | $CPIMPORTCMD $DB test009 >> import.log 2>&1
        if [ -f stop.txt ]; then
                echo "Found stop.txt.  Stopping imports."
                exit 0
        fi
done

#
# Launch script to run deletes by batch.
#
echo "Launching delete script."
./delete9.sh $DB $LOADS $ROWSPERLOAD &

#
# Launch concurrent scripts to query the table from which rows are being deleted.
#
echo "Launching query script."
for((i=1; i<=6; i++)) 
do
	./query.sh $DB $i $ROWSPERLOAD &
done
wait

#
# Gather statistics and Validate results.
#
queries=`getCount $DB "select count(*) from test009_query;"`
errors=`getCount $DB "select count(*) from test009_query where err=true;"`
incorrect=`getCount $DB "select count(*) from test009_query where correctResults=false;"`

if [ $errors -gt 0 ] || [ $incorrect -gt 0 ]; then
	echo "Failed (imports/deletes=$LOADS, rowsPerImport=$ROWSPERLOAD, queries=$queries, errors=$errors, incorrectQueries=$incorrect)" > status.txt
	exit 1
else
	echo "Passed (imports=$LOADS, rowsPerImport=$ROWSPERLOAD, queries=$queries)" > status.txt
fi

echo "All done."
exit 0
