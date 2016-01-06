#!/bin/sh
#
# Test 617.  Populate ssb100Q1c.lineorder with rows from 1992 Q1 using insert into select from.
#
TEST=test617
STATUSTEXT="617 Insert Into Select From 1992 Q1 LO : "

DB=ssb100c
DB2=ssb1992Q1c

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
# Import rows into the lineorder table.
#
table=lineorder
sec1=`date '+%s.%N'`
$MYSQLCMD $DB -vvv -e "insert into $DB2.lineorder (select * from $DB.lineorder where lo_orderdate <= 19920331);"  > $TEST.log
sec2=`date '+%s.%N'`
rows=`$MYSQLCMD $DB2 --skip-column-names -e "select count(*) from lineorder;"`
rowRate=`$MYSQLCMD $DB --skip-column-names -e "select format($rows/($sec2-$sec1), 0);"`

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

