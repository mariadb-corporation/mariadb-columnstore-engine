#!/bin/bash

#
# Concurrent update test.  Each update starts a new session, no concururrent updates against the same table.
#
# Three optional parameters:
# 1) # of tables / concurrent sessions
# 2) Rows per table.
# 3) Time to run in seconds.
#

source ../../scripts/mysqlHelpers.sh

# Constants.
db=dmlc
stopOnError=true
waitCount=60 # seconds to wait before a thread is considered hung

# Defaults - overridden by optional parameters.
threads=15
rows=1000 # Rows per table / size of each update
secondsToRun=180

if [ $# -gt 0 ]; then
	threads=$1
fi
if [ $# -gt 1 ]; then
	rows=$2
fi
if [ $# -gt 2 ]; then
	secondsToRun=$3
fi

echo "Running with threads=$threads, rowsPerTable=$rows, secondsToRun=$secondsToRun"
echo ""
sleep 2

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

#
# Executes the sql statement defined in the sql variable.
#
runSQL() {
	dt=`date '+%Y-%m-%d %H:%M:%S|%N' | awk '{print substr($0, 1, length($0)-3)}'`
	# thread|datetime|milliseconds|sql|
	# echo "$thr|$dt|$sql|before" >> thread.$thr.log
	echo "`date` - Running $sql"
	echo "`date '+%Y-%m-%d %H:%M:%S.%N'` before" >> thread.$thr.log
	$MYSQLCMD $db -vvv -n -e "$sql" >> thread.$thr.log 2>&1
	ret=$?
	if $stopOnError && [ $ret -ne 0 ]; then

		# IGNORING getModuleStatus error for now.
		# TODO:  Take this out once we have bug 5181 fixed.
#		lines=`grep ERROR thread.$thr.log | egrep -v "getModuleStatus|readToMagic" | wc -l`
		lines=`grep ERROR thread.$thr.log | egrep -v "readToMagic" | wc -l`
		if [ $lines -gt 0 ]; then
			echo "Error detected, touch stop.txt"
			touch stop.txt
		fi
	fi
	echo "`date '+%Y-%m-%d %H:%M:%S.%N'` after" >> thread.$thr.log
}

create() {
	sql="create table if not exists $tbl(batch int, c1 int, c2 int)engine=infinidb;"
	runSQL
}

load() {
	useImport=1
	if [ $useImport -eq 1 ]; then
		echo "`date` - Importing rows:  batch=$batch; table=$tbl; rows=$rows."
                sql="truncate table $tbl;"
                runSQL
		echo "" | awk -v rows=$rows -v batch=$batch '{for(i=1; i<=rows; i++)print batch "|" i "|\\N|"}' | /usr/local/Calpont/bin/cpimport $db $tbl > thread.$thr.log 2>&1
	else
		echo "" | awk -v rows=$rows -v batch=$batch '{for(i=1; i<=rows; i++)print batch "|" i "|\\N|"}' > /tmp/$tbl.tbl
		sql="truncate table $tbl; load data infile '/tmp/$tbl.tbl' into table $tbl fields terminated by '|';"
		runSQL
	fi 
}

update() {
	sql="update $tbl set c2=$batch;"
	runSQL
}

#
# Loop loading batches of rows.  Run by each thread.
#
doStuff() {
       thr=$1
       tbl=$2
       batch=0
       while [ true ]; do
               let batch++;
               update
               if [ -f stop.txt ]; then
                       echo "`date` - Thread $thr found stop.txt.  Exiting."
                       echo "`date` - All done." >> thread.$thr.log
                       exit
               fi
       done
}


createAndLoad() {
	for((i=1; i<=$threads; i++)); do
		echo "`date` - Creating and loading $tbl."
		tbl=test211_$i
		thr=$i
		create
		count=`getCount $db "select count(*) from $tbl;"`
		if [ $count -eq $rows ]; then
			echo "`date` - $tbl already loaded."
		else
			load
		fi
done
}

#
# Main.  Launch threads and run for secondsToRun.
#
rm -f thr*log
rm -f stop.txt

# Create databae.
$MYSQLCMD -e "create database if not exists $db;"

# Create and load the tables.
batch=0
createAndLoad

# Loop and spawn threads.
echo "`date` - $threads threads will run for $secondsToRun seconds."
for((i=1; i<=$threads; i++)); do
	doStuff $i test211_$i &
done

# Sleep for the specified time.
echo "`date` - Main thread sleeping for up to $secondsToRun seconds."

secondsRemaining=$secondsToRun
while [ $secondsRemaining -gt 0 ]; do
	sleep 5
        if [ -f stop.txt ]; then
               	echo "`date` - Main thread found stop.txt.  Breaking."
                break
       	fi
	let secondsRemaining=$secondsRemaining-5;
	echo "`date` - Main thread checked for stop.txt - $secondsRemaining seconds remaining." 
	
	#
	# Look for hung threads.  If the last line in the log file is like the one below and over a minute old, call it hung.
	# 2013-03-20 12:09:32.009365015 before
	#
	for((i=1; i<= $threads; i++)); do
		lastLine=`tail -1 thread.$i.log`
		if [ "`echo $lastLine | grep before`" ]; then
			dt=`echo $lastLine | awk '{print substr($0, 1, 19)}'`
			secondsOld=`$MYSQLCMD --skip-column-names -e "select timestampdiff(SECOND, '$dt', now());"`
			if [ $secondsOld -gt $waitCount ]; then
				echo "`date` - Main thread detected that thread $i has had no activity for $secondsOld seconds.  Touching stop.txt."
				touch stop.txt
				break
			fi
		fi
	done	

done

# Touch stop.txt to tell other threads to finish and quit.
if [ $secondsRemaining -le 0 ]; then
	echo "`date` - Main thread touching stop.txt to stop the test."
	touch stop.txt
fi

# Loop and check for other threads being done.
rtn=1
secondsRemaining=$waitCount
while [ $secondsRemaining -gt 0 ]; do
	sleep 2
	count=`grep "All done" thread.*.log | wc -l`
	if [ $count -eq $threads ]; then
		echo "`date` - All threads complete."
		rtn=0;
		break;
	fi
	let secondsRemaining=$secondsRemaining-2;
	echo "`date` - Main thread check.  $count of $threads complete.  $secondsRemaining seconds until considered hung." 
done

# If threads were not all completed.
hungCount=0
if [ $rtn -eq 1 ]; then
	echo "`date` - Consider it hung."
	for((j=1; j<=$threads; j++)); do
		good=`grep "All done" thread.$j.log | wc -l`
		if [ $good -lt 1 ]; then
			echo "`date` - Thread $j considered hung after $waitCount checks."
			echo "`date` - Thread $j considered hung after $waitCount checks." >> thread.$j.log
			let hungCount++;
		fi
	done
fi

# Look for bad counts and errors.
badCount=`grep bad thread*log | grep -v select | wc -l`
if [ $badCount -gt 0 ];then
	rtn=1
fi
errCount=`grep -i error thread*log | grep -v select | wc -l`
if [ $errCount -gt 0 ];then
	rtn=1
fi


# Get total batches and totalRows.
totalUpdates=`grep rows thr*log | wc -l`
totalRows=`grep rows thr*log | awk '{tot+=$3}END{print tot}'`
	
if [ $rtn -eq 1 ]; then
	echo "(seconds=$secondsToRun, thr=$threads, rows=$rows, updates=$totalUpdates, rowsUpdated=$totalRows, bad count=$badCount, errors=$errCount, hung threads=$hungCount)" > status.txt
else
	echo "(seconds=$secondsToRun, thr=$threads, rows=$rows, updates=$totalUpdates, rowsUpdated=$totalRows)" > status.txt
fi

echo "All done."
exit $rtn

