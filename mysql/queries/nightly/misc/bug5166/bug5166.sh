#!/bin/bash

. /root/genii/mysql/queries/nightly/scripts/common.sh

db=dmlc
tbl=bug5166
threads=8
batches=50
rowsPerBatch=1000

echo "1) Creating table $db.$tbl."
createSql="create database if not exists $db; use $db; drop table if exists $tbl; create table $tbl(thread int, batch int, c1 int)engine=infinidb;"
$MYSQLCMD -e "$createSql" > create.log 2>&1

echo "2) Loading source data."
for((i=1; i<=$threads; i++)); do
	echo | awk -v thr=$i -v rows=$rowsPerBatch '{for(i=1; i<=rows; i++)print thr "|" 0 "|" i "|"}' > /tmp/$tbl.$i.tbl
done

echo "3) Building sql scripts."
for((i=1; i<=$threads; i++)); do
	rm -f thr.$i.sql
	for((j=1; j<=$batches; j++)); do
		echo "\! echo 'Loading batch $j at `date`'" >> thr.$i.sql
		echo "load data infile '/tmp/$tbl.$i.tbl' into table $tbl fields terminated by '|'; select sleep(.2);" >> thr.$i.sql
	done
done

echo "4) Launching $threads concurrent sessions doing $batches loads each of $rowsPerBatch rows."
for((i=1; i<=$threads; i++)); do
	$MYSQLCMD $db -vvv -n < thr.$i.sql > thr.$i.sql.log 2>&1 &
done

echo ""
echo "Waiting for scripts to complete."
wait

count=`grep -i error thr*log | wc -l`

if [ $count -gt 0 ]; then
	echo ""
	echo "There were $count errors - see thr*log."
fi
echo ""
echo "All done."



