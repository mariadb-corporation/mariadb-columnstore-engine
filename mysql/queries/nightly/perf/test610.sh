#!/bin/sh
#
# Test 610.  Mode 3 import of ssb100cM3 dimension tables.
#
TEST=test610
STATUSTEXT="610 Mode 3 Import Ssb100cM3 Dim Tables : " 
DB=ssb100cM3

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
for table in customer dateinfo part supplier; do
	for i in `$PGMPATH/calpontConsole getsystemnet | grep pm | grep -v Console | awk '{print $6}'`; do
		./remote_command.sh $i qalpont! "$PGMPATH/cpimport $DB $table /usr/local/Calpont/data/bulk/data/import/$table.tbl -m3" &
	done
	wait
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

