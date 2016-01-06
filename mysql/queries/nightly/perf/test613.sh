#!/bin/sh
#
# Test 613.  Populate ssb dimension tables with select | cpimport.
#
TEST=test613
STATUSTEXT="613 Mode 1 Select | Import Dim Tables  : "
DB=ssb100c
DB2=ssb1992c

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
	$MYSQLCMD $DB --skip-column-names -q -e "select * from $table;" | $PGMPATH/cpimport $DB2 $table -s '\t' -n1 >> $TEST.log
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

