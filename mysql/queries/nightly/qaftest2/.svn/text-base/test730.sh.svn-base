#!/bin/sh
#
# Test 730.  Do some DDL and DML on 600 million lineitem rows.
#
TEST=test730
STATUSTEXT="730 tpch100c.lineitem DDL/DML      : "
DB=tpch100c
        
echo "$STATUSTEXT In Progress" > $TEST.status

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

# Set the VersionBufferFileSize to 5GB.
echo "Setting VersionBufferFileSize to 5GB."
/usr/local/Calpont/bin/calpontConsole stopsystem y
../scripts/setConfig.sh VersionBuffer VersionBufferFileSize 5GB
/usr/local/Calpont/bin/calpontConsole startsystem
cp /usr/local/Calpont/etc/Calpont.xml $TEST/Calpont.xml.5G


#
# Run validation queries.  Use the test615 folder for the dimension table validation.
#
echo "Running queryTester with some statements that will take quite a while."
../../queryTester -q test730 -u $DB -r Tr -g > $TEST.log 2>&1

#
# Populate the .status file.
#
errorCount=`grep -i error $TEST.log | wc -l`
grep failed $TEST.log > failed.txt
failedCount=`cat failed.txt | wc -l`

if [ $errorCount -gt 0 ]
then
        echo "$STATUSTEXT Failed (Check $TEST.log for errors)" > $TEST.status
elif [ $failedCount -gt 0 ]
then
        echo "$STATUSTEXT Failed (Data validation failed.  See details below.)" > $TEST.status
else
        echo "$STATUSTEXT Passed" > $TEST.status
fi

# List the individual sql validation scripts that failed in the .details file.
if [ $failedCount -gt 0 ]
then
        echo "$TEST validation scripts that failed:" > $TEST.details
        cat failed.txt >> $TEST.details
fi

# Set the VersionBufferFileSize back to 1GB.
echo "Setting VersionBufferFileSize to 1GB."
/usr/local/Calpont/bin/calpontConsole stopsystem y
../scripts/setConfig.sh VersionBuffer VersionBufferFileSize 1GB
# Remove the version buffer to be safe.  Not sure what would happen if it was allowed to grow larger then the size was set back down.
# Of course, mignt not want to zap it if there was a problem above and a transaction left open.  Oh well, sunny day.
rm -f /usr/local/Calpont/data*/vers*
/usr/local/Calpont/bin/calpontConsole startsystem
cp /usr/local/Calpont/etc/Calpont.xml $TEST/Calpont.xml.1G

cat $TEST.status

