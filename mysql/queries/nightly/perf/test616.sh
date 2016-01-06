#!/bin/sh
#
# Test 616.  Populate ssb dimension tables with insert into select from.
#
TEST=test616
STATUSTEXT="616 Insert Into Select From Dim Tables : "
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
# Import rows into the four dimension tables.
#
rm -f $TEST.log
for table in customer dateinfo part supplier; do
	echo "Inserting into $table."
	$MYSQLCMD $DB2 -vvv -e "insert into $DB2.$table (select * from $DB.$table);"  >> $TEST.log
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

