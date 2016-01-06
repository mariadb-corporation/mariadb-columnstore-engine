#!/bin/sh
#
# Test 501.  Query performance test.
#
TEST=test501
STATUSTEXT="501 Query Performance  :             "

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

if [ $# -ge 1 ]
then
	runName=$1
else
	runName=`pwd | awk -F "/" '{print $3}'`
fi



cd $TEST
./go501.sh $runName
cd ..

cat $TEST/aaa_summary.txt > $TEST.details

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt))" > $TEST.status

cat $TEST.status

