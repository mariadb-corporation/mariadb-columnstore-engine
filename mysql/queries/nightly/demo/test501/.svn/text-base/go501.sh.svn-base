#!/bin/sh
#
# This script runs the sql statements in the folder, validates results, and tracks timings and other statistics.
#
# Notes:
# - Files containing the select statement are q0001.sql, q0002.sql, and so on.  This script only picks up the times beginning
#   with the fourth query in each file, then every third one thereafter so as to ignore the calgetstats(), calflushcache() times, 
#   etc.  Follow this pattern if adding more files query files.
#
# - A subdirectory will be created with the results.  The subdirectory name includes the date and the passed parm.  
#   For example, running the script such as "./go.sh nightly" on 7/1/09 will create a subdirectory named 20090701-nightly.
#

DB=`../getDatabaseName.sh tpch1tc`
if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

# Get the date and name for the directory that will contain the log files.
dt=`date '+%Y%m%d'`
dir=`date '+%Y-%m-%d@%H:%M:%S'`

# Make the directory that will contain all of the log files.  It is named as YYYYMMDD-<parm 1> such as 20090715-nightly.
mkdir -p $dir >/dev/null 2>&1

rm -f *.log
rm -f *.txt
rm -rf $dir/*

# Add the Calpont.xml file, versions, and system status.
cp /usr/local/Calpont/etc/Calpont.xml $dir
/usr/local/Calpont/bin/calpontConsole getsystemstatus > $dir/systemstatus.txt
/usr/local/Calpont/bin/calpontConsole getcalpontsoftwareinfo > $dir/swversions.txt

# Run the queries.
for i in q*.sql; do
	echo "Running $i."
	$MYSQLCMD $DB -vvv < $i > $i.log 2>&1
	cp $i.log $dir
done

# Add a text file with the query times and the total at the end of the file.
# Notes:
# - The log files contain output other query times that we don't want
#   from the calls to calgetstats(), calflushcache(), and now().  They are set up so that the first "real" query is the fourth
#   in the sql file, then every third one after that (e.g. #4, #7, #10, ...).  This is the reason for the row logic below.
#
# - The output shows minutes and seconds if the time was over a minute, otherwise just seconds are shown.  This is the reason
#   for the if condition below.
#   Examples:
#     231 rows in set (42.89 sec)
#     277 rows in set (1 min 25.35 sec)
for i in q*.sql.log; do
	grep sec $i | awk '{row++;if (row > 3 && row %3 == 1)if ($6=="min")print substr($5,2,5)*60+$7;else if ($1=="Empty") print substr($3,2,5); else print substr($5,2,5)'}
done | awk '{ttl+=$1;print $1}END{print "Total Seconds = " ttl}' > $dir/qtimes.$dt.txt

# Do a diff for the query results.
./diff.sh $dir baseline > qdiff.results

#
# Save off files with the physical i/o, block touches and cp blocks.  These next three loop work against output from 
# select calgetstats() that looks like this:
# tpch06_modified.sql.log:| Query Stats: MaxMemPct-1; NumTempFiles-0; TempFileSpace-0MB; ApproxPhyI/O-0; CacheI/O-4013550; 
# BlocksTouched-4010083; PartitionBlocksEliminated-4966790; MsgBytesIn-13MB; MsgBytesOut-0MB; Mode-Distributed| 1245264960 90106 |
#
# Save off text file with the physical i/o.
for i in q*.sql.log; do
        grep Mem $i | awk '{x=split($7, a, "-"); print substr(a[2],1,length(a[2])-1)}'
done | awk '{ttl+=$1;print $1}END{print "Total Physical Blocks Read = " ttl}' > $dir/phyio.$dt.txt

# Save off a text file with the block touches.
for i in q*.sql.log; do
        grep Mem $i | awk '{x=split($9, a, "-"); print substr(a[2],1,length(a[2])-1)}'
done | awk '{ttl+=$1;print $1}END{print "Total Blocks Touched = " ttl}' > $dir/blockstouched.$dt.txt

# Save off a text file with the cp blocks.
for i in q*.sql.log; do
        grep Mem $i | awk '{x=split($10, a, "-"); print substr(a[2],1,length(a[2])-1)}'
done | awk '{ttl+=$1;print $1}END{print "Total CP Blocks Eliminated = " ttl}' > $dir/cpblocks.$dt.txt

# Save the aaa_summary.txt file with Green/Yellow/Red status.
./getsummary.sh > aaa_summary.txt
cp aaa_summary.txt $dir/aaa_summary.txt

# Create the archive directory if it's not already there.
mkdir -p archive >/dev/null 2>&1

# Keep 10 results folders around.  Archive the rest.
for i in `ls -d 20*`; do
	echo $i
done | awk '
{
	count++; 
	a[count]=$1
}END{
	if(count > 10)
	{
		for(i=1; i<=count-10; i++)
		{
			print "mv " a[i] " archive/."
		}
	}
}' > temp.sh
chmod 755 temp.sh
./temp.sh

# Clean up temp files.
rm -f temp.sh
rm -f temp.txt

#
# Create status.txt with results.
#
success=`grep Success qdiff.results | wc -l`
if [ $success -eq 1 ]
then
	echo "Passed (`grep Query aaa_summary.txt  | grep Total`" > status.txt
else
	echo "Failed (`grep Query aaa_summary.txt  | grep Total`" > status.txt
fi

# Output results.
echo "Script completed."
cp /var/log/Calpont/crit.log $dir
