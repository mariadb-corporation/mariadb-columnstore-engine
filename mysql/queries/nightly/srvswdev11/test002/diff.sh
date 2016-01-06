#!/bin/bash
count=32;

if [ $1 -gt 0 ]
then
	count=$1
fi

for (( i=1; i <= $count; i++ ))
do
	for fl in *.sql; do
		diff baseline/$fl.log $i/$fl.log -b > /dev/null
		if [ $? -ne 0 ]; then
			echo "$i/$fl.log does not match baseline/$fl.log."
		fi
	done
done


