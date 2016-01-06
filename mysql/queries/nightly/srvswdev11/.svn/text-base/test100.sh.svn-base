#!/bin/bash
#
# Test 100.  Rebuilds the tables used in the other tests w/ compression on.
#
TEST=test100
STATUSTEXT="100 Create/Load Compressed Tables:   "

. ../scripts/common.sh $1

echo "$STATUSTEXT In Progress" > $TEST.status

TPCHDB=tpch1c
SSBDB=ssbc
DMLDB=dmlc

rm -rf test100
cp -r test000 test100

# 
# Rebuild the databases used by the nightly tests with compression on.
#
$MYSQLCMD -e "set global infinidb_compression_type=1;"
cd $TEST
./rebuildDatabase.sh compressed > ../$TEST.log 2>&1
cd ..

#
# Run validation queries.
#
../../queryTester -p $QUERYTESTER_P -q $TEST -t dmlSchemaValidation -u $DMLDB -r Tr -g >> $TEST.log
../../queryTester -p $QUERYTESTER_P -q $TEST -t ssbSchemaValidation -u $SSBDB -r Tr -g >> $TEST.log
../../queryTester -p $QUERYTESTER_P -q $TEST -t tpchSchemaValidation -u $TPCHDB -r Tr -g >> $TEST.log

#
# Populate the .status file.
#
errorCount=`grep -i error $TEST.log | wc -l`
grep failed $TEST.log > failed.txt
failedCount=`cat failed.txt | wc -l`

if [ $errorCount -gt 0 ]
then
        echo "$STATUSTEXT Failed (Check $TEST.log for errors)" > $TEST.status
elif [ $failedCount -gt 0 ]
then
        echo "$STATUSTEXT Failed (Data validation failed.  See details below.)" > $TEST.status
		ret=1
else
        echo "$STATUSTEXT Passed" > $TEST.status
		ret=0
fi

# List the individual sql validation scripts that failed in the .details file.
if [ $failedCount -gt 0 ]
then
        echo "$TEST validation scripts that failed:" > $TEST.details
        cat failed.txt >> $TEST.details
fi

cat $TEST.status
exit $ret

