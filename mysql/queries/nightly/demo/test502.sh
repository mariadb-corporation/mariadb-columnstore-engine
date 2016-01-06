#!/bin/sh
#
# Test 502.  DML/DDL performance test.
#
TEST=test502
STATUSTEXT="502 DML/DDL Performance  :           "

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

if [ $# -ge 1 ]
then
	runName=$1
else
	runName=`pwd | awk -F "/" '{print $3}'`
fi

cd $TEST
./go502.sh $runName
cd ..

cat $TEST/aaa_summary.txt > $TEST.details

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt))" > $TEST.status

cat $TEST.status

