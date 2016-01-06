#!/bin/sh
script_dir=$(dirname $0)
cd $script_dir

. ../scripts/common.sh
source ../scripts/oamCommands.sh
source ../scripts/mysqlHelpers.sh
source ../scripts/testResults.sh

# Create an archive directory.
# Get the date and name for the directory that will contain the log files.
dir=`date '+%Y-%m-%d@%H:%M:%S'`
mkdir -p ./archive/$dir
rm -f *.log
rm -f *.status
rm -f *.details

# Set the NumCores setting to 8 if running on demo stack.
match=`/usr/local/Calpont/bin/calpontConsole getSystemNet | grep "srvdemo1" | wc -l`
if [ $match -ge 1 ]; then
	echo "Setting PrimitiveServers/NumCores to 8 in Calpont.xml."
	WRITENODEPMNUM=`/usr/local/Calpont/bin/calpontConsole getsystemstatus | grep Active | awk '{print substr($7, 2, 3)}'`
	WRITENODENAME=`/usr/local/Calpont/bin/calpontConsole getsystemnet | grep $WRITENODEPMNUM | grep Performance | awk '{print $6}'`

	/usr/local/Calpont/bin/remote_command.sh $WRITENODENAME Calpont1 "/usr/local/Calpont/bin/setConfig PrimitiveServers NumCores 8"
	echo "Sleeping for 60 seconds."
	sleep 60
	echo "Restarting system."
	#/usr/local/Calpont/bin/calpontConsole stopSystem y
	#/usr/local/Calpont/bin/calpontConsole startSystem y
	#/usr/local/Calpont/mysql/mysql-Calpont restart
	echo "Sleeping for 10 seconds."
	sleep 10
	echo ""
fi

#
# Run the tests populating go.log with the status from each as we go.
#
startLoggingRun demo
for test in test*.sh; do
        testName=`echo $test | awk '{print substr($1, 1, 7)}'`
        startLoggingTest $runId $testName
        echo "Running $test."
        ./$test
        stopLoggingTest $runId $testName
done
stopLoggingRun $runId

#
# Populate go.log with status of each test.
#
echo "" > go.log
/usr/local/Calpont/bin/calpontConsole getcalponts | egrep "Build Date|Version" >> go.log
echo "" >> go.log
cat test*.status >> go.log

#
# Add summaries for time and max memory usage.
#
echo "" >> go.log
$MYSQLCMD testresults --table < ../scripts/testResults/query1.sql >> go.log
echo "" >> go.log
#$MYSQLCMD testresults --table < ../scripts/testResults/query1.5.sql >> go.log
#echo "" >> go.log
$MYSQLCMD testresults --table < ../scripts/testResults/query2.sql >> go.log
echo "" >> go.log
$MYSQLCMD testresults --table < ../scripts/

testResults/query3.sql >> go.log

#
# Add the details from each test to go.log.
#
for details in test*.details; do
	echo "" >> go.log
	echo "-----------------------------------------------------------------------------------------------------------" >> go.log
	echo "" >> go.log
	cat $details >> go.log	
done

#
# Output completed message to go.log.
#
echo "" >> go.log
echo "-----------------------------------------------------------------------------------------------------------" >> go.log
echo "" >> go.log
echo "Tests completed!" >> go.log

# Copy log files off to the archive directory.
cp *.log ./archive/$dir
cp *.status ./archive/$dir
cp *.details ./archive/$dir
cp /usr/local/Calpont/etc/Calpont.xml ./archive/$dir
cp /var/log/Calpont/crit.log ./archive/$dir
cp /var/log/Calpont/warning.log ./archive/$dir
/usr/local/Calpont/bin/calpontConsole getcalpontsoftwareinfo > ./archive/$dir/swinfo.txt

#
# Export the results tables.
#
rm -f /tmp/run.tbl /tmp/testrun.tbl /tmp/testrunmem.tbl
$MYSQLCMD testResults -e "select * into outfile '/tmp/run.tbl' from run;"
$MYSQLCMD testResults -e "select * into outfile '/tmp/testrun.tbl' from testrun;"
$MYSQLCMD testResults -e "select * into outfile '/tmp/testrunmem.tbl' from testrunmem;"
tar cvzf ./archive/$dir/results.tgz /tmp/*run*tbl
rm -f /tmp/run.tbl /tmp/testrun.tbl /tmp/testrunmem.tbl

