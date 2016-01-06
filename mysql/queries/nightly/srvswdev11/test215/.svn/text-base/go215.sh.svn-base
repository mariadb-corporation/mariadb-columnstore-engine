#!/bin/bash

#
# Quick hack for Cindy to have a test that runs one long session for each thread instead of sessions for each statement.
#

#
# DML concurrency test.  Runs multiple threads repeating the following:
# 1) load a batch of rows (the number of rows varies by thread).
# 2) delete the rows with autocommitt off, then rollback the delete the delete.
# 3) Updates the c2 column.
# 4) Validates the count for the batch just loaded.
#
# The number of threads is defined by the threads variable assigned below.
# The test runs for the numberOfSeconds variable assigned below.
# 

# Constants.
#secondsToRun=300
batchesPerThread=30
db=dmlc
threads=6
rowFactor=10*1000 # Batch size will be thread number 1..n * rowFactor

#
# Executes the sql statement defined in the sql variable.
#
runSQL() {
	echo $sql >> thread.$thr.sql
}


create() {
	sql="drop table if exists $tbl; create table $tbl(batch int, c1 int, c2 int)engine=infinidb;"
	runSQL
}

load() {
	echo "" | awk -v rows=$rows -v batch=$batch '{for(i=1; i<=rows; i++)print batch "|" i "|\\N|"}' > /tmp/$tbl.$batch.tbl
	sql="load data infile '/tmp/$tbl.$batch.tbl' into table $tbl fields terminated by '|';"
	runSQL
}

deleteWithRollback() {
	sql="set autocommit=off; delete from $tbl where batch=$batch; rollback; set autocommit=on;"
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

runThread() {
	thr=$1
	echo "`date` - Thread $thr starting."
	$MYSQLCMD $db -f -vvv < thread.$thr.sql > thread.$thr.log 2>&1
	echo "`date` - Thread $thr finished."
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
	for((batch=1; batch<=$batchesPerThread; batch++)); do	
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
rm -f thr*sql
rm -f stop.txt
rm -f /tmp/test215*tbl

# Create databae.
$MYSQLCMD -e "create database if not exists $db;"

# Loop and spawn threads.
echo "`date` - $threads threads will run for $batchesPerThread batches."
for((i=1; i<=$threads; i++)); do
	let rows=$i*$rowFactor;
	doStuff $i test215_$i $rows &
done

# Run the sql scripts in parallel
for((i=1; i<=$threads; i++)); do
	runThread $i &
done
wait

rtn=0

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
for((i=1; i<=$threads; i++)); do
	echo ""
	echo "Table:  test215_$i:"
	sql="select batch, min(c1), max(c1), count(c1), min(c2), max(c2), count(c2) from test215_$i group by 1 order by 1;"
	$MYSQLCMD $db --table -e "$sql"
done

# Get total batches and totalRows.
totalBatches=0
totalRows=0
for((i=1; i<=$threads; i++)); do
	batches=`$MYSQLCMD $db -e "select count(distinct batch) from test215_$i;" | tail -1`
	let totalBatches+=batches;
	rows=`$MYSQLCMD $db -e "select count(*) from test215_$i;" | tail -1`
	let totalRows+=rows;
done
	
if [ $rtn -eq 1 ]; then
	echo "(thr=$threads, rowFactor=$rowFactor, batches=$totalBatches, rows=$totalRows, bad count=$badCount, errors=$errCount)" > status.txt
else
	echo "(thr=$threads, rowFactor=$rowFactor, batches=$totalBatches, rows=$totalRows)" > status.txt
fi

echo "All done."
exit $rtn

