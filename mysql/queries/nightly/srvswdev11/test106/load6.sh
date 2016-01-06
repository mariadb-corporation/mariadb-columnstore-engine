#!/bin/bash

#
# Repeats load data infiles into test006 table.
# Parms:
#   DB
#     database to run against.
#   LOADs
#     Number of load data infiles to run.
#   ROWSPERLOAD
#     Number of rows for each load.

if [ $# -lt 3 ]
then
        echo "load6.sh db loads rowsPerLoad - requires three parms."
        exit
fi
DB=$1
LOADS=$2
ROWSPERLOAD=$3
TABLE=test006
LOADLOGTABLE=test006_load


#
# Clear out old sql load log file.
#
rm -f load.sql.log

# 
# Loop and repeat loads.
#
for((batch=1; batch<=$LOADS; batch++))
do
	dt=`date '+%Y-%m-%d %H:%M:%S'`
	echo "dummy" | awk -v batch=$batch -v dt="$dt" -v rows=$ROWSPERLOAD '{ for (i=1; i<=rows; i++){print batch "|" dt "|" i "|" i "|" i "|" i "|" i}; }' > /tmp/test006.tbl
	echo "Running load batch $batch of $LOADS at $dt." 
	$MYSQLCMD $DB -e "insert into $LOADLOGTABLE values ($batch, 'load batch $batch', now(), null);"
	$MYSQLCMD $DB -vvv -n < load.sql >> load.sql.log 2>&1
	$MYSQLCMD $DB -e "update $LOADLOGTABLE set stop=now() where id=$batch;"
	if [ -f stop.txt ]; then
		echo "Found stop.txt.  Load script stopping."
		exit
	fi
done

touch stop.txt

echo "Load script complete."

