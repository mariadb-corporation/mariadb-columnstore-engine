#!/bin/sh
#
# Test 607.  Mode 2 import of ssb100cM2.lineorder.
#
TEST=test607
STATUSTEXT="607 Mode 2 Import Ssb100cM2 Dim Tables : "
DB=ssb100cM2

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
rm -f $TEST.log
for table in customer dateinfo part supplier; do
	echo "Running mode 2 import into $DB.$table."
	$PGMPATH/cpimport $DB $table -m2 -l $table.tbl >> $TEST.log
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

