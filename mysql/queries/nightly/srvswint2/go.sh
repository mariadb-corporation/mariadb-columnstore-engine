#!/bin/sh
#
# This script runs the sql statements in the folder, saves off results, and reports a status of green, yellow, or red.
#
# Notes:
# - Files containing the select statement are q0001.sql, q0002.sql, and so on.  This script only picks up the times beginning
#   with the fourth query in each file, then every third one thereafter so as to ignore the calgetstats(), calflushcache() times, 
#   etc.  Follow this pattern if adding more files query files.
#
# - Files containing DDL/DML statements are m0001.sql, m0002.sql, and so on.  Every time from these files is recorded.
#
# - The DDL/DML files clean up after themselves by dropping any tables that are created and rolling back any changes 
#   against existing tables.
#
# - A subdirectory will be created with the results.  The subdirectory name includes the date and the passed parm.  
#   For example, running the script such as "./go.sh nightly" on 7/1/09 will create a subdirectory named 20090701-nightly.
#

# Get the date and name for the directory that will contain the log files.
dt=`date '+%Y%m%d'`
dir=`date '+%Y%m%d'`-$1

#cd /root/genii/mysql/queries/nightly/demo

# Make the directory that will contain all of the log files.  It is named as YYYYMMDD-<parm 1> such as 20090715-nightly.
if [ ! -d $dir ]
then
	mkdir $dir
fi 

rm -rf aaa_summary.txt
rm -rf *.log
rm -rf $dir/*

# Add the Calpont.xml file, versions, and system status.
cp /usr/local/Calpont/etc/Calpont.xml $dir/.
/usr/local/Calpont/bin/calpontConsole getsystemstatus > $dir/systemstatus.txt
/usr/local/Calpont/bin/calpontConsole getcalpontsoftwareinfo > $dir/swversions.txt

# Run the queries.
for i in `ls q*.sql`
do
	echo "Running $i."
	/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root tpch100 -vvv < $i > $i.log 2>&1
	cp $i.log $dir/.
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
for i in `ls q*.sql.log`
do
	grep sec $i | awk '{row++;if (row > 3 && row %3 == 1)if ($6=="min")print substr($5,2,5)*60+$7;else if ($1=="Empty") print substr($3,2,5); else print substr($5,2,5)'}
done > temp.txt
cat temp.txt | awk '{ttl+=$1;print $1}END{print "Total Seconds = " ttl}' > $dir/qtimes.$dt.txt

#
# Save off files with the physical i/o, block touches and cp blocks.  These next three loop work against output from 
# select calgetstats() that looks like this:
# tpch06_modified.sql.log:| Query Stats: MaxMemPct-1; NumTempFiles-0; TempFileSpace-0MB; ApproxPhyI/O-0; CacheI/O-4013550; 
# BlocksTouched-4010083; PartitionBlocksEliminated-4966790; MsgBytesIn-13MB; MsgBytesOut-0MB; Mode-Distributed| 1245264960 90106 |
#
# Save off text file with the physical i/o.
for i in `ls q*.sql.log`
do
        grep Mem $i | awk '{x=split($7, a, "-"); print substr(a[2],1,length(a[2])-1)}'
done > temp.txt
cat temp.txt | awk '{ttl+=$1;print $1}END{print "Total Physical Blocks Read = " ttl}' > $dir/phyio.$dt.txt

# Save off a text file with the block touches.
for i in `ls q*.sql.log`
do
        grep Mem $i | awk '{x=split($9, a, "-"); print substr(a[2],1,length(a[2])-1)}'
done > temp.txt
cat temp.txt | awk '{ttl+=$1;print $1}END{print "Total Blocks Touched = " ttl}' > $dir/blockstouched.$dt.txt

# Save off a text file with the cp blocks.
for i in `ls q*.sql.log`
do
        grep Mem $i | awk '{x=split($10, a, "-"); print substr(a[2],1,length(a[2])-1)}'
done > temp.txt
cat temp.txt | awk '{ttl+=$1;print $1}END{print "Total CP Blocks Eliminated = " ttl}' > $dir/cpblocks.$dt.txt

# Run the DDL/DML scripts.
for i in `ls m*.sql`
do
	echo "Running $i."
	/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root tpch100 -vvv -f < $i > $i.log 2>&1
	cp $i.log $dir/.
done

# Save the DDL/DML times off to a file.
for i in `ls m*.sql.log`
do
	grep sec $i | awk '{if ($7=="min")print substr($6,2,5)*60+$8;else print substr($6,2,5)'}
done > temp.txt
cat temp.txt | awk '{ttl+=$1;print $1}END{print "Total Seconds = " ttl}' > $dir/mtimes.$dt.txt

# Save the aaa_summary.txt file with Green/Yellow/Red status.
./getsummary.sh > aaa_summary.txt
cp aaa_summary.txt $dir/aaa_summary.txt

# Create the archive directory if it's not already there.
if [ ! -d archive ]
then
	mkdir archive
fi 

# Keep 10 results folders around.  Archive the rest.
for i in `ls -d 20*`
do
	echo $i
done > temp.txt
cat temp.txt | awk '
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
rm -rf temp.sh
rm -rf temp.txt

# Output results.
head -5 aaa_summary.txt
echo "Script completed."
