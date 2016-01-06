#!/bin/bash
#
# Test 200.  Monitors rows limits based on Calpont.xml TotalUmMemory setting at 4G.
#
. ../scripts/common.sh $1

TEST=test200
STATUSTEXT="200 Monitor TotalUmMemory:           "

$MYSQLCMD -e "set global infinidb_compression_type=1;"

# Default the status to Failed.
echo "$STATUSTEXT In Progress" > $TEST.status

#
# Set TotalUmMemory to 4G if necessary
#
totalUmMemory=`../scripts/getConfig.sh HashJoin TotalUmMemory`
if [ "$totalUmMemory" != "4G" ]; then
	echo "Setting TotalUmMemory to 4G."
	../scripts/setConfig.sh HashJoin TotalUmMemory 4G

	if $WINDOWS; then
		../scripts/restart.sh
	else
	       	# Kill ExeMgr as a quick poor man's restartsystem.
        	pkill ExeMgr
	        sleep 10
	fi
fi

cd $TEST
./go200.sh
cd ..

#
# Set TotalUmMemory back to it's original value if necessary.
#
if [ "$totalUmMemory" != "4G" ]; then
	echo "Setting TotalUmMemory back to $totalUmMemory."
	../scripts/setConfig.sh HashJoin TotalUmMemory $totalUmMemory

	if $WINDOWS; then
		../scripts/restart.sh
	else
       		# Kill ExeMgr as a quick poor man's restartsystem.
	        pkill ExeMgr
        	sleep 10
	fi
fi

#
# Populate the .status file.
#
diffs=`wc -l $TEST/diff.txt | awk '{print $1}'`
if [ $diffs -gt 0 ]; then
        echo "$STATUSTEXT Failed (check $TEST/diff.txt)" > $TEST.status
		ret=1
else
        echo "$STATUSTEXT Passed" > $TEST.status
		ret=0
fi

cat $TEST.status
exit $ret

