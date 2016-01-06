#!/bin/sh
#
# Test 510.  Repeats joins at the edge of TotalUmMemory set to 4G to monitor memory. 
#
TEST=test510
STATUSTEXT="510 Repeat UM Join Query:            "

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

# Default the status to Failed.
echo "$STATUSTEXT Incomplete" > $TEST.status

cd $TEST
./go510.sh
cd ..

#
# Populate the .status file.
#
diffs=`wc -l $TEST/diff.txt | awk '{print $1}'`
if [ $diffs -gt 0 ]; then
        echo "$STATUSTEXT Failed (check $TEST/diff.txt)" > $TEST.status
else
        echo "$STATUSTEXT Passed" > $TEST.status
fi

cat $TEST.status

