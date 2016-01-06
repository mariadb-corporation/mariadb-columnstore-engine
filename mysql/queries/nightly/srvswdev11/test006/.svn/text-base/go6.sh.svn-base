#!/bin/bash

DB=dml
LOADS=100
ROWSPERLOAD=25000

source ../../scripts/mysqlHelpers.sh

rm -f logs/*
rm -f stop.txt

if [ $# -eq 1 ]; then
	DB=$1
fi

$MYSQLCMD $DB -vvv < create.sql > logs/create.sql.log

#
# Launch script to run repeated loads.
#
echo "Launching load script."
./load6.sh $DB $LOADS $ROWSPERLOAD &
sleep 10

#
# Launch concurrent scripts to query the table being loaded.
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
loads=`grep "rows affected" load.sql.log | wc -l`
queries=`getCount $DB "select count(*) from test006_query;"`
errors=`getCount $DB "select count(*) from test006_query where err=true;"`
# Look for errors in the create / load log if none were reported in the queries.
if [ $errors -eq 0 ]; then
        errors=`grep -i error *log | wc -l`
fi
incorrect=`getCount $DB "select count(*) from test006_query where correctResults=false;"`

if [ $errors -gt 0 ] || [ $incorrect -gt 0 ]; then
	echo "Failed (loads=$loads, rowsPerLoad=$ROWSPERLOAD, queries=$queries, errors=$errors, incorrectQueries=$incorrect)" > status.txt
	ret=1
else
	echo "Passed (loads=$loads, rowsPerLoad=$ROWSPERLOAD, queries=$queries)" > status.txt
	ret=0
fi

echo "All done."
exit $ret

