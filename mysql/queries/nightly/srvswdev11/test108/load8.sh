#!/bin/bash
#
# Repeats imports into test008 table.
# Parms:
#   DB
#     database to run against.
#   LOADs
#     Number of imports to run.
#   ROWSPERLOAD
#     Number of rows for each import.

source ../../scripts/mysqlHelpers.sh

if [ $# -lt 3 ]
then
        echo "load8.sh db loads rowsPerLoad - requires three parms."
        exit
fi
DB=$1
LOADS=$2
ROWSPERLOAD=$3
TABLE=test008
IMPORTLOGTABLE=test008_import

#
# Clear out old sql import log file.
#
rm -f import.log
rm -f stop.txt

# 
# Loop and repeat imports.
#
$INSTALLDIR/bin/colxml $DB -t $TABLE -j 20 > import.log 2>&1
batch=`getCount $DB "select ifnull(max(batch)+1,1) from $TABLE"`
for((i=1; i<=$LOADS; i++))
do
	echo "" >> import.log
	dt=`date '+%Y-%m-%d %H:%M:%S'`
	echo "Running import batch $batch of $LOADS at $dt." 
	$MYSQLCMD $DB -e "insert into $IMPORTLOGTABLE values ($i, 'import batch $batch', now(), null);"
	echo "dummy" | awk -v batch=$batch -v dt="$dt" -v rows=$ROWSPERLOAD '{ for (i=1; i<=rows; i++){print batch "|" dt "|" i "|" i "|" i "|" i "|" i}; }' | 	$CPIMPORTCMD $DB $TABLE >> import.log 2>&1
	$MYSQLCMD $DB -e "update $IMPORTLOGTABLE set stop=now() where id=$batch;"
	let batch++;
	if [ -f stop.txt ]; then
		echo "Found stop.txt.  Load script stopping."
		exit
	fi
done
touch stop.txt

echo "Load script complete."

