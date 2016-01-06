#!/bin/sh
#
# Test 731.  Monitor time of individual .sql scripts.
#
TEST=test731
STATUSTEXT="731 Time Statements                : "
       
. ../scripts/common.sh $1
 
echo "$STATUSTEXT In Progress" > $TEST.status

cd $TEST
./go731.sh
results=$?
cd ..

# Report the status.
echo "$STATUSTEXT `cat $TEST/status.txt`" > $TEST.status

echo "731 Time Statements Details:" > $TEST.details
echo "" >> $TEST.details
cat $TEST/details.txt >> $TEST.details

cat $TEST.status
cat $TEST.details
