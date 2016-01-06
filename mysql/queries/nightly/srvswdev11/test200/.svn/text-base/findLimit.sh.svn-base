#!/bin/bash

. ../../scripts/common.sh

#
# Runs query doing a binary search w/ the where clause value to find the largest value allowed without exceeding TotalUmMemory Calpont.xml setting.
#

MAX=150000000
high=$MAX
low=1
limit=0
done=0
db=dmlc

#mid=$((($high - $low + 1)/2))
mid=$(($low + ($high - $low)/2))		
while [ $done -eq 0 ]
do
	if [ $mid -ge $high ] || [ $mid -le $low ]
	then
		done=1
	fi
#	sql="select count(distinct c1) from test200 where c1 <= $mid;"
#	sql="select count(*) as count16 from test200 a join test200 b using (c3) where a.c1 <= $mid;"
#	sql="select count(a.c2) as count8 from test200 a join test200 b on a.c2 = b.c2 where a.c2 <= $mid;"
#	sql="update test200 x set x.c2=(select sub.c1 from test200b sub where sub.c1 <= $mid and sub.c1 = x.c1) where x.c1 <= $mid;"
#	sql="select count(*) as count11 from (select c1 from test200 where c1 <= $mid union select c1 from test200 where c1 <= 1000000) x;"
	sql="select count(distinct c1) as count3 from test200 where c1 <= $mid;"


	echo $sql
        $MYSQLCMD $db -vvv -e "$sql" > temp.log 2>&1
        cat temp.log
        toobig=`grep "memory limit" temp.log | wc -l`
        error=`grep ERROR temp.log | wc -l`
	if [ $toobig -gt 0 ]
	then
		high=$mid
		echo "$mid was too big.  Range is now $(($high-$low+1)).  Try lower."
	elif [ $error -gt 0 ]
	then
                echo "Error running sql.  Exiting."
                exit
	else
		limit=$mid
		low=`expr $mid + 1`
		echo "$mid was okay.  Range is now $(($high-$low+1)).  Try higher."
		limitSQL=$sql
	fi		
	echo ""
	mid=$(($low + ($high - $low)/2))		
	sleep 1
done

echo ""
echo "The limit is:"
echo $limitSQL
echo ""
echo "Memory settings:"
egrep "TotalUm|PmMax" $INSTALLDIR/etc/Calpont.xml
