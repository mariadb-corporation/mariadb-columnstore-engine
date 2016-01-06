#!/bin/sh
#
# Test 603.  Mode 0 distribute ssb dimenstion tables.
#
TEST=test603
STATUSTEXT="603 Mode 0 Distribute Lineorder        : "

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
table=lineorder
echo "Distributing $table."
for i in `$PGMPATH/calpontConsole getsystemnet | grep pm | grep -v Console | awk '{print $6}'`; do
       	$PGMPATH/remote_command.sh $i qalpont! "rm -f /usr/local/Calpont/data/bulk/data/import/$table.tbl" >> $TEST.log
done

sec1=`date '+%s.%N'`
$PGMPATH/cpimport $DATASOURCEPATH/$table.tbl -m0 >> $TEST.log
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

