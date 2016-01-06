#!/bin/sh
#
# Test 503.  Concurrent fast / slow query test.
#
TEST=test503
STATUSTEXT="503 Fast / Slow Concurrent:          "

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go503.sh 
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt))" > $TEST.status

cat $TEST.status

