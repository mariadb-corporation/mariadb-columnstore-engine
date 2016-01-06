#!/bin/bash
#
# Imports ssb 100 database, runs validation queries, and verifies the casual partition limits set in the extent map by 
# cpimport and by the query scans.
#
. ../scripts/common.sh

SCHEMA=ssb100
#READBUFFERSIZE=1048576
#WRITEBUFFERSIZE=1048576
READBUFFERSIZE=16777216
WRITEBUFFERSIZE=16777216

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

#-------------------------------------------------------------------------------
# main entry point to this script
#-------------------------------------------------------------------------------
rm -f /usr/local/Calpont/data/bulk/data/import
ln -s /mnt/data/ssb/100G /usr/local/Calpont/data/bulk/data/import

/usr/local/Calpont/bin/colxml $SCHEMA -j 300 -c $READBUFFERSIZE -w $WRITEBUFFERSIZE -t customer -t dateinfo -t part -t supplier 

# Get the starting MB_wrtn from iostat for the DBRoots.
getMBWritten
startingMBWritten=$totMB # totMB set by getMBWritten
echo $startingMBWritten > starting.io.$SCHEMA.txt

# Import dimension tables.
./trackMem.sh > mem.log 2>&1 &
disown

#
# Run the imports and track the time.
#
sec1=`date '+%s.%N'`
/usr/local/Calpont/bin/cpimport -j 300 

# Import lineorder fact table.
for i in /mnt/data/ssb/100G/line*; do
	echo "Importing $i." 
	/usr/local/Calpont/bin/cpimport $SCHEMA lineorder $i 
	echo "" 
done
sec2=`date '+%s.%N'`
runTime=`$MYSQLCMD $DB --skip-column-names -e "select format($sec2-$sec1, 2);"`

pkill trackMem.sh
maxMem=`awk '{if($4>max)max=$4}END{print max "%"}' mem.log`
avgMem=`awk '{ttl=ttl+$4; count++}END{print ttl/count "%"}' mem.log`
rm -f mem.log

# Get the total MB_wrtn from iostat for the DBRoots.
getMBWritten
endingMBWritten=$totMB # totMB set by getMBWritten
echo $endingMBWritten > ending.io.$SCHEMA.txt
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

