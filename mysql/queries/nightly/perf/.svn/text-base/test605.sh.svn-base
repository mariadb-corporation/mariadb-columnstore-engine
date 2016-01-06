#!/bin/sh
#
# Test 605.  Mode 1 import of ssb100c.lineorder.
#
TEST=test605
STATUSTEXT="605 Mode 1 Import Ssb100c.lineorder    : "
DB=ssb100c

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
table=lineorder
sec1=`date '+%s.%N'`
$PGMPATH/cpimport $DB $table $DATASOURCEPATH/$table.tbl > $TEST.log
sec2=`date '+%s.%N'`
rowRate=`$MYSQLCMD $DB --skip-column-names -e "select format(600037902/($sec2-$sec1), 0);"`

#
# Populate the .status file.
#
errorCount=`grep -i error $TEST.log | wc -l`

if [ $errorCount -gt 0 ]
then
        echo "$STATUSTEXT Failed (Check $TEST.log for errors)" > $TEST.status
else
        echo "$STATUSTEXT Passed ($rowRate rows per second)" > $TEST.status
fi

cat $TEST.status

