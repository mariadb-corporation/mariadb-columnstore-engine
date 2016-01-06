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
if $WINDOWS; then
	ARCHIVEDIR=$WINDRIVE/nightly/srvswdev11/archive/$dir
else
	ARCHIVEDIR=./archive/$dir
fi
mkdir -p $ARCHIVEDIR
rm -f *.log
rm -f *.status
rm -f *.details

#
# Set cpimport priority high and PrimProc / ExeMgr priories low to speed things up on the concurrent import tests. 
#
echo "Updating Calpont.xml settings."
../scripts/setConfig.sh ExeMgr1 Priority 1 > setConfig.log 2>&1
../scripts/setConfig.sh PrimitiveServers Priority 1 >> setConfig.log 2>&1
../scripts/setConfig.sh WriteEngine Priority 40  >> setConfig.log 2>&1

../scripts/setConfig.sh CrossEngineSupport Host localhost  >> setConfig.log 2>&1
../scripts/setConfig.sh CrossEngineSupport User root  >> setConfig.log 2>&1

#
# Set the block cache % to 10% for single server.
#
if ! `isMultiNode`; then
	../scripts/setConfig.sh DBBC NumBlocksPct 10 >> setConfig.log 2>&1
fi

echo "Restarting system."
restartSystem

#
# Run the tests populating go.log with the status from each as we go.
#
hostName=`hostname | awk '{print substr($0, 1, 8)}'`
if [ "$hostName" == "qaftest7" ]; then
	../perf/clearOut.sh > clearOut.log 2>&1
fi

#
# Set tests to run.  We are skipping some of the non compressed tests on Windows as the full suite takes forever.
#
if $WINDOWS; then
	tests="test000.sh test001.sh"
#	tests="test000.sh test001.sh test011.sh test100.sh test101.sh test105.sh test102.sh test106.sh test107.sh test108.sh test109.sh test110.sh test112.sh test2*.sh"
else
        tests="test000.sh test001.sh"	
#	tests="test200.sh test201.sh"
fi

startLoggingRun srvswdev11
for test in $tests; do
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
if $WINDOWS; then

	#
	# Add a prefix to go.log indicating the Build date and test time
	#
	svnBranch=`grep -m1 branch $WINDRIVE/Calpont/etc/CalpontVersion.txt | awk '{print $2}'`
	bldSystem=`grep "Build Host" $WINDRIVE/Calpont/etc/CalpontVersion.txt | awk '{print $7}'`
	echo "###########################################################################" > go.log
	echo "   Calpont InfiniDB Windows Edition" >> go.log
	echo "   Release=$version.$release SVN-BRANCH=$svnBranch" >> go.log
	echo "   Compiled Date=$sBuildDtm  Build System=$bldSystem " >> go.log
	echo "   Automated Build Tester Report $(date)" >> go.log
	echo "###########################################################################" >> go.log
	echo "" >> go.log
else
	echo "" > go.log
	/usr/local/Calpont/bin/calpontConsole getcalponts | egrep "Build Date|Version" >> go.log
fi
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
if ls test*.details &> /dev/null; then
	for details in test*.details; do
		echo "" >> go.log
		echo "-----------------------------------------------------------------------------------------------------------" >> go.log
		echo "" >> go.log
		cat $details >> go.log	
	done
fi

#
# Output completed message to go.log.
#
echo "" >> go.log
echo "-----------------------------------------------------------------------------------------------------------" >> go.log
echo "" >> go.log
echo "Tests completed!" >> go.log
echo "Archive directory $ARCHIVEDIR" >> go.log

# Copy log files off to the archive directory.
cp *.log $ARCHIVEDIR
cp *.status $ARCHIVEDIR
if ls test*.details &> /dev/null; then
	cp test*.details $ARCHIVEDIR
fi
cp $INSTALLDIR/etc/Calpont.xml $ARCHIVEDIR
if ! $WINDOWS; then
	cp /var/log/Calpont/crit.log $ARCHIVEDIR
	cp /var/log/Calpont/warning.log $ARCHIVEDIR
	/usr/local/Calpont/bin/calpontConsole getcalpontsoftwareinfo > $ARCHIVEDIR/swinfo.txt
else
	cp $INSTALLDIR/log/InfiniDBLog.txt $ARCHIVEDIR
	cp $INSTALLDIR/etc/CalpontVersion.txt $ARCHIVEDIR
fi

