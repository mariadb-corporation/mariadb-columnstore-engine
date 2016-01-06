#!/bin/sh

#
# Clears out a database.  Internal use only.
#
script_dir=$(dirname $0)

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

if [ -z "$PGMPATH" ]; then
	PGMPATH="/usr/local/Calpont/bin"
fi

match=`$PGMPATH/calpontConsole getSystemNet | egrep "srvqaperf2|srvalpha2|srvprodtest1" | wc -l`
if [ $match -ge 1 ]; then
        PASSWORD=qalpont!
else
        PASSWORD=Calpont1
fi

$MYSQLCMD --skip-column-names -e "show databases;" | egrep -v "information_schema|calpontsys|archive|infinidb_vtable|querystats|mysql|test|testresults" > schemas.txt

echo "Stopping system."
$PGMPATH/calpontConsole stopsystem y

#
# Remove the front end schemas.
#
echo ""
echo "Removing front end schemas."
for i in `cat schemas.txt`; do
	echo "rm -rf /usr/local/Calpont/mysql/db/$i"
	rm -rf /usr/local/Calpont/mysql/db/$i
done
echo ""

#
# For each PM, clear the data files and shared memory segments.
#
for i in `$PGMPATH/calpontConsole getsystemnet | grep pm | grep -v Console | awk '{print $6}'`; do
	echo "Clearing shared memory on $i."
	$script_dir/remoteCommand.sh $i $PASSWORD "$PGMPATH/clearShm"
	echo "Removing data files on $i."
	$script_dir/remoteCommand.sh $i $PASSWORD "rm -rf /usr/local/Calpont/data*/000.dir"
	echo "Removing bulkRollback files on $i."
	$script_dir/remoteCommand.sh $i $PASSWORD "rm -rf /usr/local/Calpont/data*/bulkRollback/*"
	echo "Removing versionbuffer files on $i."
	$script_dir/remoteCommand.sh $i $PASSWORD "rm -rf /usr/local/Calpont/data*/versionbuffer.cdf"
	echo "Removing dbrm files on $i."
	$script_dir/remoteCommand.sh $i $PASSWORD "rm -rf /usr/local/Calpont/data1/systemFiles/dbrm/*"
	echo "Clearing shared memory again on $i."
	$script_dir/remoteCommand.sh $i $PASSWORD "$PGMPATH/clearShm"
done

#
# Clear shared memory on the UM.
#
echo "Clearing shared memory on this node."
$PGMPATH/clearShm
echo ""

#
# Start the system from the write node.
#
WRITENODEPMNUM=`$PGMPATH/calpontConsole getsystemstatus | grep Active | awk '{print substr($7, 2, 3)}'`
if [ -z $WRITENODEPMNUM ]; then
        WRITENODEPMNUM='pm1'
fi
WRITENODENAME=`$PGMPATH/calpontConsole getsystemnet | grep $WRITENODEPMNUM | grep Performance | awk '{print $6}'`

echo "Starting system."
$script_dir/remoteCommand.sh $WRITENODENAME $PASSWORD "$PGMPATH/calpontConsole startsystem"

active=0
while [ $active -eq 0 ]; do
	sleep 1
	active=`$PGMPATH/calpontConsole getsystemstatus | grep System | grep ACTIVE | wc -l`
done

echo "Running dbbuilder 7 on the write node."
$script_dir/remoteCommand.sh $WRITENODENAME $PASSWORD "$PGMPATH/dbbuilder 7"

echo ""
echo "All done!"
