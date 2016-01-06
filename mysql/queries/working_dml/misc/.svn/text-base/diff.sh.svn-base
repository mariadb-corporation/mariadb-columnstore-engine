#!/bin/sh
for i in `ls *.sql`
do
	if [ -f $i.log ]
	then
		diff $i.ref.log $i.log > diff.txt
		lines=`cat diff.txt | wc -l`
		if [ $lines -eq 0 ]
		then
			echo "MATCH    $i"
		else
			echo "MISMATCH $i"
		fi
	fi
done
rm -f diff.txt
