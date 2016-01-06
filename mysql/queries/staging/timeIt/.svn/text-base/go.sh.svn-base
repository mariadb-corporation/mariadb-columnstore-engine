#!/bin/bash
. /root/genii/mysql/queries/nightly/scripts/common.sh
source /root/genii/mysql/queries/nightly/scripts/mysqlHelpers.sh

DB=walt

#
# Create table for logging sql statement results
#
sql="
create database if not exists $DB;  
use $DB;  
drop table if exists runs;
create table runs(queryId int, run int, statement varchar(200), time float, avgExePct float, maxExePct float, avgPPPct float, maxPPPct float, success bool);
"
$MYSQLCMD -e "$sql"

#
# Create table for tracking memory.
#
$MYSQLCMD $DB < /root/genii/tools/calpontSupport/analytics/create.sql

echo ""

#
# Loop through sql file and run statements.
#
timesToRun=2
queryId=0
while read statement; do
	let queryId++;
	for((i=1; i<=$timesToRun; i++)); do
		#
		# Start script to track memory and run the sql statement.
		#
		./track.sh > /tmp/track.log &
		disown # to avoid the "Terminated" message when we kill track.sh later in the loop.
        	start=`date +%s.%N`
		echo "Query ID         : $queryId"
		echo "Query Run        : $i"
		echo "Query            : $statement" 
		success=1
        	$MYSQLCMD -e "$statement" > /dev/null  
		if [ $? -ne 0 ]; then
			success=0
		fi
	        stop=`date +%s.%N`
        	runTime=`$MYSQLCMD --skip-column-names -e "select round($stop-$start,6);"`
		sleep .5
		pkill track.sh

		#
		# Load the memory logs into processLog table.
		#
		sql="
			truncate table processLog; 
			load data infile '/tmp/track.log' into table processLog fields terminated by '|';
		"
		$MYSQLCMD $DB -e "$sql"

		#
		# Get the memory results for ExeMgr and PrimProc.
		#
		avgExe=`getCount $DB "select avg(processMemPct) from processLog where processCommand='ExeMgr';"`
		maxExe=`getCount $DB "select max(processMemPct) from processLog where processCommand='ExeMgr';"`
		avgPP=`getCount $DB "select avg(processMemPct) from processLog where processCommand='PrimProc';"`
		maxPP=`getCount $DB "select max(processMemPct) from processLog where processCommand='PrimProc';"`
		
		#
		# Log the results for the sql statement
		#
		sql="
			insert into runs values ($queryId, $i, '$statement', $runTime, $avgExe, $maxExe, $avgPP, $maxPP, $success);
		"
		$MYSQLCMD $DB -e "$sql"
		echo "Time             : $runTime seconds"
		echo "ExeMgr Avg Mem   : $avgExe%"
		echo "ExeMgr Max Mem   : $maxExe%" 
		echo "PrimProc Avg Mem : $avgPP%" 
		echo "PrimProc Max Mem : $maxPP%"
		echo
	done
done < queries.sql

$MYSQLCMD $DB --table -e "select * from runs;"
	
echo "All done."
