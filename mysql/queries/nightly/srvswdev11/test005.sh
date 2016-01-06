#!/bin/bash
#
# Test 5.  Uses queryTester to run the working_dml_dml folder tests.
#
. ../scripts/common.sh $1

TEST=test005
STATUSTEXT="005 Working DML Test:                "

echo "$STATUSTEXT In Progress" > $TEST.status

$MYSQLCMD -e "set global infinidb_compression_type=0;"

#
# Rebuild the dml.datatypetestm, orders, and lineitem tables on the srvswdev1 reference database and the local database.
#
#/usr/local/Calpont/mysql/bin/mysql -h 10.100.4.67 -u root -pCalpont1 --database=dml < ../../../working_dml/misc/AAA.REBUILD.REF > ../../../working_dml/misc/AAA.REBUILD.REF.log 2>&1
# 9/21/2010.  Commented out below.  All of the tables are rebuilt not in test000.sh and again with compression in test100.sh.
#/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root dml < ../../working_dml/misc/AAA.REBUILD.IDB > ../../working_dml/misc/AAA.REBUILD.IDB.log 2>&1 
#/usr/local/Calpont/bin/colxml dml -t lineitem -j 1234
#/usr/local/Calpont/bin/cpimport -j 1234

cd $TEST
./rebuild.sh
cd ..

cd ../..

# 
# Run the nighty queryTester scripts populating with results going to log file.
#
./queryTester -p $QUERYTESTER_P -q working_dml -u dml -g -d dml -r B  > nightly/srvswdev11/$TEST.log

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
echo "Working DML Test Details:" > $TEST.details
echo "" >> $TEST.details

# List individual scripts that failed.
grep failed $TEST.log > failed.txt
lines=`cat failed.txt | wc -l`
if [ $lines -gt 0 ]
then
        echo "Working DML Test scripts that failed:" > $TEST.details 
        cat failed.txt >> $TEST.details

        # Copy off the log file with a .bad.log extension.  These tests get re run for compressed tables and the log file gets overwritten.
        for i in `awk '{print $4}' failed.txt`
        do
                cp ../../$i.log ../../$i.bad.log
        done
fi

rm -f failed.txt

# Add totals from queryTester runs.
echo "" >> $TEST.details
echo "Details from queryTester:" >> $TEST.details
grep "Total Local Passed" $TEST*.log   | awk '{ttl=ttl+$5}END{print "Total Local Passed   = " ttl}' >> $TEST.details
grep "Total Local Failed" $TEST*.log   | awk '{ttl=ttl+$5}END{print "Total Local Failed   = " ttl}' >> $TEST.details
grep "Total Ref Passed" $TEST*.log     | awk '{ttl=ttl+$5}END{print "Total Ref Passed     = " ttl}' >> $TEST.details
grep "Total Ref Failed" $TEST*.log     | awk '{ttl=ttl+$5}END{print "Total Fef Failed     = " ttl}' >> $TEST.details
grep "Total Compare Passed" $TEST*.log | awk '{ttl=ttl+$5}END{print "Total Compare Passed = " ttl}' >> $TEST.details
grep "Total Compare Failed" $TEST*.log | awk '{ttl=ttl+$5}END{print "Total Compare Failed = " ttl}' >> $TEST.details
grep -i select ../../working_dml/*/*.sql | wc -l | awk '{print "Total Selects        = " $1}' >> $TEST.details
grep -i insert ../../working_dml/*/*.sql | wc -l | awk '{print "Total Inserts        = " $1}' >> $TEST.details
grep -i update ../../working_dml/*/*.sql | wc -l | awk '{print "Total Updates        = " $1}' >> $TEST.details
grep -i delete ../../working_dml/*/*.sql | wc -l | awk '{print "Total Deletes        = " $1}' >> $TEST.details
grep -i create ../../working_dml/*/*.sql | wc -l | awk '{print "Total Creates        = " $1}' >> $TEST.details
grep -i drop   ../../working_dml/*/*.sql | wc -l | awk '{print "Total Drops          = " $1}' >> $TEST.details

exit $ret
