#!/bin/bash

#
# Run mode 1 imports in parallel.
#

db=dmlc
let rowsPerImport=2*1000*1000;
importsPerPM=5
table=test206

doImports() {
	pm=$1
	rm -f import.$pm.log
	i=1	
	while [ $i -lt $importsPerPM ]; do
        	if [ -f stop.txt ]; then
        	        echo "Found stop.txt.  Exiting script for $pm."
			break
        	fi
		str="Running import $i of $importsPerPM on PM$pm at `date`." 
		echo $str >> import.$pm.log
		echo $str

		echo "" | awk -v pm=$pm -v batch=$i -v rows=$rowsPerImport '{for(i=1; i<=rows; i++)printf "%d|%d|%012d|%012d|%012d|\n", pm, batch, i, i, i;}' |
			/usr/local/Calpont/bin/cpimport $db $table -P $pm >> import.$pm.log
	        rc=$?
	        if [ $rc -ne 0 ]; then
			str="Error code $rc on import $i for table $table."
			echo $str
			echo $str >> import.$pm.log
			touch stop.txt
			break;
		else
			let i++;
	        fi
	done
}

echo "Creating $db.$table."
$MYSQLCMD -vvv < create.sql > create.sql.log 2>&1

pmCount=`/usr/local/Calpont/bin/calpontConsole getsystemstatus | grep Module | grep pm | grep ACTIVE | wc -l`
for((j=1; j<=$pmCount; j++)); do
	doImports $j &
done

wait
rm -f stop.txt

echo "All done."
