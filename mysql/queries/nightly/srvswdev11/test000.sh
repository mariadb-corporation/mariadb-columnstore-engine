#!/bin/bash
#
# Test 0.  Rebuilds the tables used in the other tests.
#
TEST=test000
STATUSTEXT="000 Create/Load Uncompressed Tables: "

. ../scripts/common.sh $1

TPCHDB=tpch1
SSBDB=ssb
DMLDB=dml

echo "$STATUSTEXT In Progress" > $TEST.status

#
# Rebuild the databases with compression off.
#
$MYSQLCMD -e "set global infinidb_compression_type=0;"
cd $TEST
./rebuildDatabase.sh > ../$TEST.log 2>&1
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
		ret=1
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

