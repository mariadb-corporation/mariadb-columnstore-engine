#!/bin/bash

#
# Drop partition test.
#

db=dml

if [ $# -eq 1 ]
then
        db=$1
fi

#
# Clear out old sql and log files.
#
rm -f *log

#
# Create the test010 table.
#
$MYSQLCMD $db -n -vvv < create.sql > create.sql.log

#
# Determine the number of rows to fill a stripe on PM1.
#
rootCount=`../../scripts/getConfig.sh SystemModuleConfig ModuleDBRootCount1-3`
extentsPer=`../../scripts/getConfig.sh ExtentMap ExtentsPerSegmentFile`
filesPer=`../../scripts/getConfig.sh ExtentMap FilesPerColumnPartition`
dbRootCount=`../../scripts/getConfig.sh SystemConfig DBRootCount`

#---------------------------------------
# Test disabling/dropping of segment files.
# Import two stripes on PM1 for main test.
#---------------------------------------
let rowCount=(rootCount*filesPer*extentsPer*8192*1024/dbRootCount)-2048;
batch=0
rm -f import.log
for((day=10; day<=11; day++)); do
	echo "Importing day $day.  Last is 11."
	let batch++;
	if $WINDOWS; then
		# No -p and -m parameters for cpimport on Windows.
		echo "" | awk -v day=$day -v batch=$batch -v rows=$rowCount '{for(i=1; i<=rows; i++)print batch "|2012-10-" day "|" i}' | $CPIMPORTCMD $db test010 >> import.log 2>&1
	else
		echo "" | awk -v day=$day -v batch=$batch -v rows=$rowCount '{for(i=1; i<=rows; i++)print batch "|2012-10-" day "|" i}' | $CPIMPORTCMD $db test010 -m 1 -P 1 >> import.log 2>&1
	fi
done

#---------------------------------------
# Setup test for bug 3838.  Note:  Did the .log.sql below on purpose, the drop partition call is expected to error off and we don't want
# the validation logic in ../test010.sh to consider that an error.
# 
# bug3838 test made obsolete with bug5237, so bug3838 test commented out.
#---------------------------------------
#$MYSQLCMD $db -n < bug3838.sql > bug3838.log.sql 2>&1

$MYSQLCMD $db -n < partitions.sql > partitions.sql.log 2>&1

#---------------------------------------
# Bug 5237
# Test disabling/dropping segment files in last physical partition.
# Also tests that import successfully loads after files are disabled or dropped.
#---------------------------------------
rm -f import5237.log
rm -f bug5237.sql.log

#
# Import physical partitions 1 and 2 on PM1; then disable partition 2
#
echo "Importing partitions 1 and 2 for bug5237 test"
let rowCount2=(rootCount*filesPer*extentsPer*8192*1024/dbRootCount)-8192;
for((batch2=1; batch2<=2; batch2++)); do
	if $WINDOWS; then
		# No -p and -m parameters for cpimport on Windows.
		echo "" | awk -v batch=$batch2 -v rows=$rowCount2 '{for(i=1; i<=rows; i++)print batch}' | $CPIMPORTCMD $db tbug5237 >> import5237.log 2>&1
	else
		echo "" | awk -v batch=$batch2 -v rows=$rowCount2 '{for(i=1; i<=rows; i++)print batch}' | $CPIMPORTCMD $db tbug5237 -m 1 -P 1 >> import5237.log 2>&1
	fi
	let rowCount2=(rootCount*filesPer*extentsPer*8192*1024/dbRootCount);
done
echo "#bug5237a test" >> bug5237.sql.log
$MYSQLCMD $db -n < bug5237a.sql >> bug5237.sql.log 2>&1

#
# Import physical partitions 3 on PM1; then drop partition 3
#
echo "Importing partition 3 for bug5237 test"
let rowCount2=(rootCount*filesPer*extentsPer*8192*1024/dbRootCount);
if $WINDOWS; then
	# No -p and -m parameters for cpimport on Windows.
	echo "" | awk -v batch=3 -v rows=$rowCount2 '{for(i=1; i<=rows; i++)print batch}' | $CPIMPORTCMD $db tbug5237 >> import5237.log 2>&1
else
	echo "" | awk -v batch=3 -v rows=$rowCount2 '{for(i=1; i<=rows; i++)print batch}' | $CPIMPORTCMD $db tbug5237 -m 1 -P 1 >> import5237.log 2>&1
fi
echo "#bug5237b test" >> bug5237.sql.log
$MYSQLCMD $db -n < bug5237b.sql >> bug5237.sql.log 2>&1

#
# Import physical partitions 4 on PM1; then enable partition 2
#
echo "Importing partition 4 for bug5237 test"
let rowCount2=(rootCount*filesPer*extentsPer*8192*1024/dbRootCount);
if $WINDOWS; then
	# No -p and -m parameters for cpimport on Windows.
	echo "" | awk -v batch=4 -v rows=$rowCount2 '{for(i=1; i<=rows; i++)print batch}' | $CPIMPORTCMD $db tbug5237 >> import5237.log 2>&1
else
	echo "" | awk -v batch=4 -v rows=$rowCount2 '{for(i=1; i<=rows; i++)print batch}' | $CPIMPORTCMD $db tbug5237 -m 1 -P 1 >> import5237.log 2>&1
fi
echo "#bug5237c test" >> bug5237.sql.log
$MYSQLCMD $db -n < bug5237c.sql >> bug5237.sql.log 2>&1

echo "All done."
exit 0
