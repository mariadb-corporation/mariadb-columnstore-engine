#!/bin/bash

# Runs the .sql files in the current working directory.  This is basically the same thing that 
# queryTester does without the comparison.  Run this, then run diffAll.sh.

. /root/genii/mysql/queries/nightly/scripts/common.sh

for fl in *.sql; do
	echo "Running $fl."
	$MYSQLCMD tpch1 -f < $fl > $fl.log 2>&1
	sleep 8
done
