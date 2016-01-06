#!/bin/sh
#
# Imports the 100 GB TPCH data into the schema passed in as the parameter.
# The eight TPCH tables must have already been created in the schema.

SCHEMA=tpch100

READBUFFERSIZE=33554432
WRITEBUFFERSIZE=33554432
READBUFFERS=3
READTHREADS=2
PARSETHREADS=5

if [ $1 ] ; then
        SCHEMA=$1
fi

#-------------------------------------------------------------------------------
# Gets the sum of the "MB_wrtn" from "iostat -m" for the DBRoots.  Used to report the total I/O for the import.
#-------------------------------------------------------------------------------
getMBWritten()
{
	totMB=0

	# Loop through the mounts that contain "Calpont".  The substr in the awk assumes mounts are like "/dev/sda1".
	for mount in ` mount | grep Calpont | awk '{print substr($1, 6, 3)}'`
	do
	        dbRootMB=`iostat -m | grep $mount | awk '{print $6}'`
        	let totMB+=$dbRootMB
	done
}

rm -rf /usr/local/Calpont/data/bulk/data/import
ln -s /mnt/data/tpch/100g /usr/local/Calpont/data/bulk/data/import
/usr/local/Calpont/bin/colxml $SCHEMA -t region -t nation -t customer -t part -t supplier -t partsupp -t orders -t lineitem -j 11 -c $READBUFFERSIZE -w $WRITEBUFFERSIZE -r $READBUFFERS 

# Get the starting MB_wrtn from iostat for the DBRoots.
getMBWritten
startingMBWritten=$totMB # totMB set by getMBWritten

./trackMem.sh > mem.log 2>&1 &
disown

#
# Run the import and track the time.
#
sec1=`date '+%s.%N'`
/usr/local/Calpont/bin/cpimport -j 11 -w $PARSETHREADS -r $READTHREADS  
sec2=`date '+%s.%N'`
runTime=`$MYSQLCMD $DB --skip-column-names -e "select format($sec2-$sec1, 2);"`

pkill trackMem.sh
maxMem=`awk '{if($4>max)max=$4}END{print max "%"}' mem.log`
avgMem=`awk '{ttl=ttl+$4; count++}END{print ttl/count "%"}' mem.log`
rm -f mem.log

# Get the total MB_wrtn from iostat for the DBRoots.
getMBWritten
endingMBWritten=$totMB # totMB set by getMBWritten
let totalMBWritten=$endingMBWritten-$startingMBWritten;

# Use awk to get decimal #, probably an easier way, but this will do.
totalGBWritten=`echo $totalMBWritten | awk '{print $1/1024}'` 

size=`/root/genii/tools/calpontSupport/schemaSizeReport.sh $SCHEMA | grep Total | awk -F "=" '{print $2}'`
echo "$SCHEMA Import Run Time:        $runTime seconds"
echo "$SCHEMA Size on Disk:           $size" 
echo "$SCHEMA Written to Disk:        $totalGBWritten GB" 
echo "$SCHEMA cpimport memory peak:   $maxMem" 
echo "$SCHEMA cpimport memory avg:    $avgMem" 

echo "Status:"
echo "($runTime sec, footprint: $size, written: $totalGBWritten GB, maxMem: $maxMem, avgMem: $avgMem)"

