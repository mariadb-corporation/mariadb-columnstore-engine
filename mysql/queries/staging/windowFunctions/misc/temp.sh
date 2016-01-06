#!/bin/bash
limit=`wc -l temp.sql | awk '{print $1}'`
for((i=1; i<=$limit; i++)); do
	suffix=`printf "%02i" $i`
	head -$i temp.sql | tail -1 > bug5366_$suffix.sql
	echo $suffix
done
