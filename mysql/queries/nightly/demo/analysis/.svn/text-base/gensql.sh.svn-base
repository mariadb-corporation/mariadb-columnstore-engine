# Script to generate sql to populate the run and queryRun tables with run time stats from the nightly performance runs.

key=1
for i in `ls -d ../archive/20*nightly-genii`
do
	date=`echo $i | awk '{print substr($1, 12, 8)}'`
	echo "insert into run values($key, '$date', true);" 

	cat $i/qt* | grep -v "Total" | awk -v key=$key '{print "insert into queryrun values (" NR ", " key ", " $1 " );"}'
	echo "" 
	cat $i/mt* | grep -v "Total" | awk -v key=$key '{print "insert into modRun values (" NR ", " key ", " $1 " );"}'
	echo "" 

	key=`expr $key + 1`
done

for i in `ls -d ../201008*`
do
	date=`echo $i | awk '{print substr($1, 4, 8)}'`
	echo "insert into run values($key, '$date', true);" 

	cat $i/qt* | grep -v "Total" | awk -v key=$key '{print "insert into queryrun values (" NR ", " key ", " $1 " );"}'
	echo "" 
	cat $i/mt* | grep -v "Total" | awk -v key=$key '{print "insert into modRun values (" NR ", " key ", " $1 " );"}'
	echo "" 

	key=`expr $key + 1`
done

# Eliminate a couple of runs that were skewed.
echo "update run set r_good=false where r_key=122;" 
echo "update run set r_good=false where r_key=57;"
