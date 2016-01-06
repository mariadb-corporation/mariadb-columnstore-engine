#!/bin/sh
#
# Test 611.  Mode 3 import of ssb100cM3.lineorder.
#
TEST=test611
STATUSTEXT="611 Mode 3 Import Ssb100cM3.lineorder  : "
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
table=lineorder
sec1=`date '+%s.%N'`

for i in `$PGMPATH/calpontConsole getsystemnet | grep pm | grep -v Console | awk '{print $6}'`; do
#	./remote_command.sh $i qalpont! "$PGMPATH/cpimport $DB $table -m3"
# NOTE:  Something up with mode 3 import being really slow when the path to the file is not specified.  Running like below for time being.
	./remote_command.sh $i qalpont! "$PGMPATH/cpimport $DB $table /usr/local/Calpont/data/bulk/data/import/lineorder.tbl -m3" &
done
wait
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

#
# Remove the import files from the PMs.
#
for i in `$PGMPATH/calpontConsole getsystemnet | grep pm | grep -v Console | awk '{print $6}'`; do
        for table in customer dateinfo part supplier lineorder; do
                $PGMPATH/remote_command.sh $i qalpont! "rm -f /usr/local/Calpont/data/bulk/data/import/$table.tbl" >> $TEST.log
        done
done

cat $TEST.status

