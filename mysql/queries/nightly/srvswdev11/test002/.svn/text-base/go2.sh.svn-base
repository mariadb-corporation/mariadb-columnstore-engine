#!/bin/bash

# Runs the passed number of concurrent group streams against 1 GB database.
#
# Parms:
#    number of concurrent sessions to run (required)
#    database (optional) defaults to tpch1 if not provided.
#
# How to call for 32 concurrent sessions:
#   ./go.sh 32

session=$1
db=tpch1

sessions=$1
if [ $# -gt 1 ]
then
        db=$2
fi

#
# Flush the PrimProc cache for starters.
#
$MYSQLCMD $db -e "select calflushcache();"

# Kick off the mysql sessions in the background and wait for them to complete.
for (( i = 1; i <= sessions; i++ ))
do
	./rungroups.sh $i $db &
done
wait

# Verify the results.
cat /dev/null > diff.txt
./diff.sh $sessions > diff.txt
lines=`cat diff.txt | wc -l`
if [ $lines -eq 0 ]
then
	echo "Success."
	./clear.sh
else
	echo "Failed.  Check diff.txt for differences."
fi
