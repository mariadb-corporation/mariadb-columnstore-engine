#!/bin/bash
#
# Test 1.  Uses queryTester to run the working folder tests.
#
TEST=test001
STATUSTEXT="001 Working Folder Test:             " 

. ../scripts/common.sh $1

$MYSQLCMD -e "set global infinidb_compression_type=0;"

echo "$STATUSTEXT In Progress" > $TEST.status

script_dir=$(dirname $0)
cd $script_dir
cd ../..

# 
# Run the nighty queryTester scripts populating with results going to log file.
#
./queryTester -p $QUERYTESTER_P -q working_ssb_compareLogOnly -r Tr -u ssb -g > nightly/srvswdev11/$TEST.log 2>&1
./queryTester -p $QUERYTESTER_P -q working_tpch1 -u tpch1 -r Tr -g >> nightly/srvswdev11/$TEST.log 2>&1
./queryTester -p $QUERYTESTER_P -q working_tpch1_calpontonly -u tpch1 -r T -g >> nightly/srvswdev11/$TEST.log 2>&1
./queryTester -p $QUERYTESTER_P -q working_tpch1_compareLogOnly -u tpch1 -r Tr -g >> nightly/srvswdev11/$TEST.log 2>&1

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
# Populate the .details file.
#

# List individual scripts that failed.
grep failed $TEST.log > failed.txt
lines=`cat failed.txt | wc -l`
if [ $lines -gt 0 ]
then
        echo "Working Folder Test scripts that failed:" > $TEST.details 
        cat failed.txt >> $TEST.details

	# Copy off the log file with a .bad.log extension.  These tests get re run for compressed tables and the log file gets overwritten.
        for i in `awk '{print $4}' failed.txt`
        do
                cp ../../$i.log ../../$i.bad.log
        done
fi
rm -f failed.txt

cat $TEST.status
exit $ret

