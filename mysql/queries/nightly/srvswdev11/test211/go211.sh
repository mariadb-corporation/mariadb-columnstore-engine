#!/bin/bash

source ../../scripts/mysqlHelpers.sh

#
# DML concurrency test.  Runs multiple threads repeating the following:
# 1) load a batch of rows (the number of rows varies by thread).
# 2) delete the rows with autocommitt off, then rollback the delete the delete.
# 3) Updates the c2 column.
# 4) Validates the count for the batch just loaded.
#
# Three optional parameters:
# 1) # of tables / concurrent sessions
# 2) Rows for table used by first thread, second thread gets rows * 2, third thread gets rows * 3, etc.
# 3) Time to run in seconds.
# 

# Constants.
db=dmlc
stopOnError=1
waitCount=180 	      # seconds to wait for all of the threads to be complete at the end of the script before a thread is considered hung
statementWaitCount=60 # seconds to wait for an individual statement or import before the thread is considered hung.

threads=15
rowFactor=10000 # Batch size will be thread number 1..n * rowFactor
secondsToRun=1800

if [ $# -gt 0 ]; then
        threads=$1
fi
if [ $# -gt 1 ]; then
        rowFactor=$2
fi
if [ $# -gt 2 ]; then
        secondsToRun=$3
fi

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
	echo "`date '+%Y-%m-%d %H:%M:%S.%N'` before sql statment" >> thread.$thr.log
	$MYSQLCMD $db -vvv -n -e "$sql" >> thread.$thr.log 2>&1
	ret=$?
	if [ $stopOnError -eq 1 ] && [ $ret -ne 0 ]; then

		# IGNORING getModuleStatus error for now.
		# TODO:  Take this out once we have bug 5181 fixed.
#		lines=`grep ERROR thread.$thr.log | egrep -v "getModuleStatus|readToMagic" | wc -l`
		lines=`grep ERROR thread.$thr.log | wc -l`
		if [ $lines -gt 0 ]; then
			echo "Error detected, touch stop.txt"
			touch stop.txt
		fi
	fi
	echo "`date '+%Y-%m-%d %H:%M:%S.%N'` after sql statement" >> thread.$thr.log
}


create() {
	sql="drop table if exists $tbl; create table $tbl(batch int, c1 int, c2 int)engine=infinidb;"
	runSQL
}

load() {
	# set loadMode:
	# 0 - use load data infile only
	# 1 - use import only
	# 2 - use both
	loadMode=2;  

	# Use loadMode to determine where to use cpimport or L.D.I. for this load.
	if [ $loadMode -eq 0 ]; then
		useImport=false
	elif [ $loadMode -eq 1 ]; then
		useImport=true
	else
		let j=($thr+$batch)%2
		if [ $j -eq 0 ];then
			useImport=true
		else
			useImport=false
		fi
	fi

	# Load the rows.
	if $useImport; then
		echo "`date` - Importing rows:  batch=$batch; table=$tbl; rows=$rows."
		echo "`date '+%Y-%m-%d %H:%M:%S.%N'` before import" >> thread.$thr.log
		echo "" | awk -v rows=$rows -v batch=$batch '{for(i=1; i<=rows; i++)print batch "|" i "|\\N|"}' | $CPIMPORTCMD $db $tbl >> thread.$thr.log 2>&1
		echo "`date '+%Y-%m-%d %H:%M:%S.%N'` after import" >> thread.$thr.log
	else
		echo "" | awk -v rows=$rows -v batch=$batch '{for(i=1; i<=rows; i++)print batch "|" i "|\\N|"}' > /tmp/$tbl.tbl
		sql="load data infile '/tmp/$tbl.tbl' into table $tbl fields terminated by '|';"
		runSQL
	fi 
}

deleteWithRollback() {
	sql="set autocommit=off; delete from $tbl where batch=$batch; rollback;"
	runSQL
}

update() {
	sql="update $tbl set c2=c1 where batch=$batch;"
	runSQL
}

validateBatch() {
	sql="select count(*), if(count(*)=$rows, 'good', 'bad') result from $tbl where batch=$batch;"
	runSQL
}

#
# Loop loading batches of rows.  Run by each thread.
#
doStuff() {
	thr=$1
	tbl=$2
	rows=$3
	batch=0
	create
	while [ true ]; do
		let batch++;
		load	
		deleteWithRollback
		update
		validateBatch
	        if [ -f stop.txt ]; then
                	echo "`date` - Thread $thr found stop.txt.  Exiting."
			echo "`date` - All done." >> thread.$thr.log
	                exit
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

# Loop and spawn threads.
echo "`date` - $threads threads will run for $secondsToRun seconds."
for((i=1; i<=$threads; i++)); do
	let rows=$i*$rowFactor;
	doStuff $i test211_$i $rows &
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
	echo ""
	echo "`date` - Main thread checked for stop.txt - $secondsRemaining seconds remaining." 
	echo ""

        #
        # Look for hung threads.  If the last line in the log file is like the one below and over $statementWaitCount seconds old, call it hung.
        # 2013-03-20 12:09:32.009365015 before
        #
        for((i=1; i<= $threads; i++)); do
                lastLine=`tail -1 thread.$i.log`
                if [ "`echo $lastLine | grep before`" ]; then
                        dt=`echo $lastLine | awk '{print substr($0, 1, 19)}'`
                        secondsOld=`getCount $db "select timestampdiff(SECOND, '$dt', now());"`
                        if [ $secondsOld -gt $statementWaitCount ]; then
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
	echo "`date` - Main thread check.  $count of $threads threads complete.  $secondsRemaining seconds until considered hung." 
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

# Output counts by batch for each table.
outputCounts=false
if $outputCounts; then
	for((i=1; i<=$threads; i++)); do
		echo ""
		echo "Table:  test211_$i:"
		sql="select batch, min(c1), max(c1), count(c1), min(c2), max(c2), count(c2) from test211_$i group by 1 order by 1;"
		$MYSQLCMD $db --table -e "$sql"
	done
fi

# Get total batches and totalRows.
totalBatches=0
totalRows=0
for((i=1; i<=$threads; i++)); do
	batches=`getCount $db "select count(distinct batch) from test211_$i;"`
	let totalBatches+=batches;
	rows=`getCount $db "select count(*) from test211_$i;"`
	let totalRows+=rows;
done
	
if [ $rtn -eq 1 ]; then
	echo "(seconds=$secondsToRun, thr=$threads, rowFactor=$rowFactor, batches=$totalBatches, rows=$totalRows, bad count=$badCount, errors=$errCount, hung threads=$hungCount)" > status.txt
else
	echo "(seconds=$secondsToRun, thr=$threads, rowFactor=$rowFactor, batches=$totalBatches, rows=$totalRows)" > status.txt
fi

echo "All done."
exit $rtn

