#!/bin/sh
#
# Test 602.  Mode 0 distribute ssb dimenstion tables.
#
TEST=test602
STATUSTEXT="602 Mode 0 Distribute Dim Tables       : "

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

if [ -z "$PGMPATH" ]; then
	PGMPATH="/usr/local/Calpont/bin"
fi

if [ -z "$DATASOURCEPATH" ]; then
	DATASOURCEPATH="/root/ssb"
fi

echo "$STATUSTEXT In Progress" > $TEST.status

#
# Import rows into the four dimension tables.
#

# TODO:  Remove hard-coded paths.

rm -f $TEST.log
for table in customer dateinfo part supplier; do
	echo "Distributing $table."
	for i in `$PGMPATH/calpontConsole getsystemnet | grep pm | grep -v Console | awk '{print $6}'`; do
        	$PGMPATH/remote_command.sh $i qalpont! "rm -f /usr/local/Calpont/data/bulk/data/import/$table.tbl" >> $TEST.log
	done
	$PGMPATH/cpimport $DATASOURCEPATH/$table.tbl -m0 >> $TEST.log
done

#
# Populate the .status file.
#
errorCount=`grep -i error $TEST.log | wc -l`

if [ $errorCount -gt 0 ]
then
        echo "$STATUSTEXT Failed (Check $TEST.log for errors)" > $TEST.status
else
        echo "$STATUSTEXT Passed" > $TEST.status
fi

cat $TEST.status

