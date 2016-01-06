#!/bin/bash

ROWS=50000
UPDATES=100
DB=dml

if [ $# -eq 1 ]
then
        DB=$1
fi

# Create the test007 table.
echo "Creating test007 table."
$MYSQLCMD $DB -e "create database if not exists $DB;" > create.sql.log 2>&1
$MYSQLCMD $DB < create.sql > create.sql.log 2>&1

# Build the import file.
rm -f /tmp/test007.tbl
head -1 create.sql | awk -v rows=$ROWS '{for(i=1; i<=rows; i++){print i "|" 0}}' > /tmp/test007.tbl

# Load the rows.
echo "Loading data into test007."
rm -f load.sql.log
$MYSQLCMD $DB -vvv -n < load.sql > load.sql.log

# Build the counts.sql file. 
rm -f counts.sql
for((i=1; i<=10; i++)); do
	echo "select sum(c2), (case sum(c2) when $ROWS then 'good' when 0 then 'good' else 'bad' end) as result from test007;" >> counts.sql
done

# Kick off four count.sh scripts in the background.
rm -f stop.txt
for((i=1; i<=4; i++)); do
	./counts.sh $i $DB &
done

# Run the update script multiple times.
rm -f update.sql.log
for((i=1; i<=$UPDATES; i++)); do
        echo "Running update script $i of $UPDATES at `date`."
        $MYSQLCMD $DB -vvv < update.sql >> update.sql.log 2>&1
        if [ -f stop.txt ]; then
                echo "Found stop.txt.  Update script $session stopping at `date`."
		break
        fi
done

touch stop.txt

# Wait for query scripts to finish.
wait

# Populate status.txt with results.
eval $(cat *sql.log* | tr '[:upper:]' '[:lower:]' | awk 'BEGIN{good=bad=error=0;} /good/ {good++;} /bad/ {bad++;} /error/ {error++} END{printf "good=%d\nbad=%d\nerror=%d\n", good, bad, error;}')

updates=`grep update update.sql.log | wc -l`

# If no good counts, fail it.
if [ $error -gt 0 ] || [ $good -eq 0 ] || [ $bad -gt 0 ]; then
        echo "Failed (updates=$updates, bad count=$bad, good count=$good, error count=$error)" > status.txt
		exit 1
else
        echo "Passed (updates=$updates, rowsPerUpdate=$ROWS, queries=$good)" > $TEST.status > status.txt
fi

echo "All done."
exit 0

