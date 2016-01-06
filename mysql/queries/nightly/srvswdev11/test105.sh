#!/bin/bash
#
# Test 105.  Uses queryTester to run the working_dml folder tests against dmlc (compressed database).
#
. ../scripts/common.sh $1

TEST=test105
STATUSTEXT="105 Working DML Test dmlc:           "

echo "$STATUSTEXT In Progress" > $TEST.status

$MYSQLCMD -e "set global infinidb_compression_type=1;"

if [ ! -d "$TEST" ]; then
	mkdir $TEST
fi
rm -f $TEST/*.sql
rm -f $TEST/*.sh
rm -f $TEST/*.tbl

cp test005/*.sql $TEST
cp test005/*.sh $TEST
cp test005/*.tbl $TEST

cd $TEST
./rebuild.sh dmlc
cd ..

cd ../..

# 
# Run the nighty queryTester scripts populating with results going to log file.
#
./queryTester -p $QUERYTESTER_P -q working_dml -u dmlc -g -t misc -r Tr  > nightly/srvswdev11/$TEST.log
./queryTester -p $QUERYTESTER_P -q working_dml -u dmlc -g -t qa_sub -r Tr  >> nightly/srvswdev11/$TEST.log

#
# Populate the .status file.
#
cd nightly/srvswdev11
grep failed $TEST.log > failed.txt
lines=`cat failed.txt | wc -l`
if [ $lines -eq 0 ]
then
        echo "$STATUSTEXT Passed" > $TEST.status
		ret=0
else
        echo "$STATUSTEXT Failed" > $TEST.status
		ret=1
fi
rm -f failed.txt

#
# Populate the .details file with failures if there are any.
#

# List individual scripts that failed.
grep failed $TEST.log > failed.txt
lines=`cat failed.txt | wc -l`
if [ $lines -gt 0 ]
then
        echo "Working DML Test scripts that failed:" > $TEST.details 
        cat failed.txt >> $TEST.details
fi
rm -f failed.txt
cat $TEST.status
exit $ret
