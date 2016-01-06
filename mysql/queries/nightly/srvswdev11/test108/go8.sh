#!/bin/bash

source ../../scripts/mysqlHelpers.sh

DB=dml
LOADS=200
ROWSPERLOAD=500000

rm -f logs/*

if [ $# -eq 1 ]; then
	DB=$1
fi

echo "Dropping old / creating new table."
$MYSQLCMD $DB -vvv < create.sql > logs/create.sql.log

#
# Launch script to run repeated imports.
#
echo "Launching load script."
./load8.sh $DB $LOADS $ROWSPERLOAD &
sleep 20

#
# Launch concurrent scripts to query the table being imported.
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
queries=`getCount $DB "select count(*) from test008_query;"`
errors=`getCount $DB "select count(*) from test008_query where err=true;"`
incorrect=`getCount $DB "select count(*) from test008_query where correctResults=false;"`

if [ $errors -gt 0 ] || [ $incorrect -gt 0 ]; then
	echo "Failed (imports=$LOADS, rowsPerImport=$ROWSPERLOAD, queries=$queries, errors=$errors, incorrectQueries=$incorrect)" > status.txt
	ret=1
else
	echo "Passed (imports=$LOADS, rowsPerImport=$ROWSPERLOAD, queries=$queries)" > status.txt
	ret=0
fi

echo "All done."
exit $ret

