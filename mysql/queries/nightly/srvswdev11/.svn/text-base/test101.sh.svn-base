#!/bin/bash
#
# Test 101.  Uses queryTester to run the working folder tests configured for all UM joins.  Runs with compression on. 
#
#
. ../scripts/common.sh $1

TEST=test101
STATUSTEXT="101 Working Folder UM Join Comp On:  "
echo "$STATUSTEXT In Progress" > $TEST.status

#
# Configure for UM joins.
#
../scripts/setConfig.sh HashJoin PmMaxMemorySmallSide 1
../scripts/restart.sh

# Turn compression on.
$MYSQLCMD -e "set global infinidb_compression_type=1;"

script_dir=$(dirname $0)
cd $script_dir
cd ../..

# 
# Run the nighty queryTester scripts populating with results going to log file.
#
./queryTester -p $QUERYTESTER_P -q working_ssb_compareLogOnly -r Tr -u ssbc -g > nightly/srvswdev11/$TEST.log 2>&1
./queryTester -p $QUERYTESTER_P -q working_tpch1 -u tpch1c -r Tr -g >> nightly/srvswdev11/$TEST.log 2>&1
./queryTester -p $QUERYTESTER_P -q working_tpch1_calpontonly -u tpch1c -r T -g >> nightly/srvswdev11/$TEST.log 2>&1
./queryTester -p $QUERYTESTER_P -q working_tpch1_compareLogOnly -u tpch1c -r Tr -g >> nightly/srvswdev11/$TEST.log 2>&1

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
echo "Working Folder UM Join Test Details:" > $TEST.details
echo "" >> $TEST.details

# List individual scripts that failed.
grep failed $TEST.log > failed.txt
lines=`cat failed.txt | wc -l`
if [ $lines -gt 0 ]
then
        echo "Working Folder Test scripts that failed:" >> $TEST.details 
        cat failed.txt >> $TEST.details
        echo "" >> $TEST.details
fi
rm -f failed.txt

# Add totals from queryTester runs.
echo "Details from queryTester:" >> $TEST.details
grep "Total Local Passed" $TEST*.log   | awk '{ttl=ttl+$5}END{print "Total Local Passed   = " ttl}' >> $TEST.details
grep "Total Local Failed" $TEST*.log   | awk '{ttl=ttl+$5}END{print "Total Local Failed   = " ttl}' >> $TEST.details
grep "Total Ref Passed" $TEST*.log     | awk '{ttl=ttl+$5}END{print "Total Ref Passed     = " ttl}' >> $TEST.details
grep "Total Ref Failed" $TEST*.log     | awk '{ttl=ttl+$5}END{print "Total Fef Failed     = " ttl}' >> $TEST.details
grep "Total Compare Passed" $TEST*.log | awk '{ttl=ttl+$5}END{print "Total Compare Passed = " ttl}' >> $TEST.details
grep "Total Compare Failed" $TEST*.log | awk '{ttl=ttl+$5}END{print "Total Compare Failed = " ttl}' >> $TEST.details
grep -i select ../../working_s*/*/*.sql ../../working_t*/*/*.sql | wc -l | awk '{print "Total Selects        = " $1}' >> $TEST.details
grep -i insert ../../working_s*/*/*.sql ../../working_t*/*/*.sql | wc -l | awk '{print "Total Inserts        = " $1}' >> $TEST.details
grep -i update ../../working_s*/*/*.sql ../../working_t*/*/*.sql | wc -l | awk '{print "Total Updates        = " $1}' >> $TEST.details
grep -i delete ../../working_s*/*/*.sql ../../working_t*/*/*.sql | wc -l | awk '{print "Total Deletes        = " $1}' >> $TEST.details
grep -i create ../../working_s*/*/*.sql ../../working_t*/*/*.sql | wc -l | awk '{print "Total Creates        = " $1}' >> $TEST.details
grep -i drop   ../../working_s*/*/*.sql ../../working_t*/*/*.sql | wc -l | awk '{print "Total Drops          = " $1}' >> $TEST.details

echo "" >> $TEST.details
echo "Changed this Calpont.xml setting:" >> $TEST.details
grep PmMaxMemorySmallSide $INSTALLDIR/etc/Calpont.xml >> $TEST.details

echo "" >> $TEST.details
echo "df -h output:" >> $TEST.details
df -h >> $TEST.details

#
# Configure back for PM joins.
#
../scripts/setConfig.sh HashJoin PmMaxMemorySmallSide 64M
../scripts/restart.sh

# Turn compression back on after the restart.
$MYSQLCMD -e "set global infinidb_compression_type=1;"

cat $TEST.status
exit $ret

