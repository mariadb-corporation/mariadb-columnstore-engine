#!/bin/bash

#
# Used this script to strip out blank lines and comments so that the first line in the ref logs from postgres is the sql statement.
#

for fl in *.sql.ref.log; do
	row=`egrep -n -i "select|delete|update" $fl | awk -F ':' '{print $1}' | head -1`
	if [ $row -gt 1 ]; then
		tot=`wc -l $fl | awk '{print $1}'`
		let tailRows=$tot-$row+1;
		tail -$tailRows $fl > tmp.txt
		mv tmp.txt $fl
		echo "Fixed $fl."
	fi
done
