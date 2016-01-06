#!/bin/sh
#
# Test 504.  Concurrent very fast / slow query test.
#
TEST=test504
STATUSTEXT="504 Very Fast / Slow Concurrent:     "

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go504.sh 
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt))" > $TEST.status

cat $TEST.status

