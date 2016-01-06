#!/bin/bash
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

startLoggingRun qaftest2

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
echo "-----------------------------------------------------------------------------------------------------------" >> go.log
echo "" >> go.log
$MYSQLCMD testresults --table < ../scripts/testResults/query.sql >> go.log
echo "" >> go.log
$MYSQLCMD testresults --table < ../scripts/testResults/query1.sql >> go.log
echo "" >> go.log
$MYSQLCMD testresults --table < ../scripts/testResults/query2.sql >> go.log
echo "" >> go.log
$MYSQLCMD testresults --table < ../scripts/testResults/query3.sql >> go.log

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

