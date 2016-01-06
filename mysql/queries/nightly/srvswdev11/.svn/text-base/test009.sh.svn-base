#!/bin/bash
#
# Test 9.  Do deletes while selecting counts in another session.
#
. ../scripts/common.sh $1

TEST=test009
STATUSTEXT="009 Count while deleting:            "

$MYSQLCMD -e "set global infinidb_compression_type=0;"


# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go9.sh
ret=$?
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt)" > $TEST.status

cat $TEST.status

exit $ret

