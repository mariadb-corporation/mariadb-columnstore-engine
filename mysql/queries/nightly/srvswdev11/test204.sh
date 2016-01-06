#!/bin/bash
#
# Test 204.  Tests error conditions encountered in bug 4028 where the extent map would have min and max entries not set but when the state was valid.
#            This would lead to rows not being found if searching on the extent map's column. 
#

. ../scripts/common.sh $1

TEST=test204
STATUSTEXT="204 EM Min/Max Valid cpimport Test:  " 

$MYSQLCMD -e "set global infinidb_compression_type=1;"

echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go204.sh > ../$TEST.log
ret=$?
cd ..

#
# Populate the .status file.
#
echo "$STATUSTEXT $(cat $TEST/status.txt)" > $TEST.status

cat $TEST.status
exit $ret

