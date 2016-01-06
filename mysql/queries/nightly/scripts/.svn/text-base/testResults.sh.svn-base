#!/bin/bash

. ../scripts/common.sh
source ../scripts/oamCommands.sh
source ../scripts/mysqlHelpers.sh

#
# This script contains functions for logging to the $DB tables on both the local machine and the centralized database on srvnightly.
#
DB=testResults

#------------------------------------------------------------------------------------
# Create database to track test results if necessary, get a new runId for this run,
#   and insert a run row.
# Returns:
#       Sets the runId variable to the new one.
#------------------------------------------------------------------------------------
startLoggingRun()
{
	testSuite=$1
        $MYSQLCMD -e "create database if not exists $DB;"
        $MYSQLCMD $DB < ../scripts/testResults/create.sql > ../scripts/testResults/create.sql.log 2>&1
        version=`getVersion`
        release=`getRelease`
        runId=`getCount $DB "select ifnull(max(runId),0)+1 from run;"`
        sBuildDtm=`getBuildDtm`
        $MYSQLCMD $DB -e "insert into run values($runId, '$version', '$release', now(), null, '$sBuildDtm', '$testSuite');"
}

#------------------------------------------------------------------------------------
# Update the run row with stop datetime.
# Arguments
#       $1 runId
#------------------------------------------------------------------------------------
stopLoggingRun()
{
        $MYSQLCMD $DB -e "update run set stop=now() where runId=$runId;"
}

#------------------------------------------------------------------------------------
# Adds a testRun for the given test and starts the script to track memory.
# Argument:
#       $1 runId
#       $2 testName
#------------------------------------------------------------------------------------
startLoggingTest()
{
	runId=$1
	testName=$2
	rm -f /tmp/stop.$testName.txt
	$MYSQLCMD $DB -e "insert into testRun values ($runId, '$testName', now(), null, 'In Progress');"
	../scripts/testResults/trackMem.sh $runId $testName > /tmp/$testName.mem.tbl &
}

#------------------------------------------------------------------------------------
# Adds a testRun for the given test and starts the script to track memory.
# Argument:
#       $1 runId
#       $2 testName
#------------------------------------------------------------------------------------
stopLoggingTest()
{
	runId=$1
	testName=$2
        touch /tmp/stop.$testName.txt
        $MYSQLCMD $DB -e "update testRun set stop=now(), status='`cat $testName.status`' where runId=$runId and test='$testName';"
	$MYSQLCMD $DB -e "load data infile '/tmp/$testName.mem.tbl' into table testRunMem fields terminated by '|';"
}

