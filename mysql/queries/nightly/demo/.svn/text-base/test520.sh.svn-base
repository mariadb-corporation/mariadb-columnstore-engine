#!/bin/sh
#
# Test 520.  Repeats 3 million row result with increasing group by col width.
#
TEST=test520
STATUSTEXT="520 Group By Increasing Width:       "

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

# Default the status to Failed.
echo "$STATUSTEXT Incomplete" > $TEST.status

cd $TEST
./go520.sh
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt)" > $TEST.status

cat $TEST.status
