#!/bin/bash
source ../../scripts/mysqlHelpers.sh

DB=testResults
TABLE=scriptTimeResults

rm -f sql/*.sql.log
$MYSQLCMD -e "create database if not exists $DB;"
$MYSQLCMD $DB < create.sql

# Loop through the sql files and run each one.
cd sql
for i in *.sql; do

	# Run the script and track the run time.
	start=`date +%s.%N`
	echo "Running $i at `date`."
	$MYSQLCMD -n < $i > $i.log 2>&1
	stop=`date +%s.%N`
	runTime=`$MYSQLCMD --skip-column-names -e "select round($stop-$start,6);"`

	# Validate results.
	diff $i.ref.log $i.log
	resultsDiffer=$?

	# Update results rows for this script.
	sql="
		update $TABLE
		set curTime = $runTime,
		    resultsMatch = not $resultsDiffer,
		    timeWithinMargin = ($runTime / baselineTime - 1) < (margin / 100)
		where script='$i';"
	$MYSQLCMD $DB -e "$sql"
done
cd ..

$MYSQLCMD $DB --table -e "select * from scriptTimeResults;" > details.txt

failedCount=`getCount $DB "select count(*) from $TABLE where resultsMatch = 0 or timeWithinMargin = 0;"`
runCount=`getCount $DB "select count(*) from $TABLE;"`

if [ $failedCount -gt 0 ]; then
	failedTimingCount=`getCount $DB "select count(*) from $TABLE where timeWithinMargin = 0;"`
	failedValidationCount=`getCount $DB "select count(*) from $TABLE where resultsMatch = 0;"`
	echo "Failed ($runCount scripts run, failedTiming=$failedTimingCount, failedValidation=$failedValidationCount)" > status.txt
else
	echo "Passed ($runCount scripts run)" > status.txt
fi

