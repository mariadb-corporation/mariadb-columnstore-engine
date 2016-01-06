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

DB=nightly100
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

# Create an orders table and populate it with 1992 data from tpch100.
$MYSQLCMD $DB -vvv < create.sql > create.sql.log 2>&1
for((year=1992; year<=1992; year++)); do
	for((month=1; month<=12; month++)); do
		let nextyear=year+1;
		sql="select * from orders where o_orderdate >= '$year-01-01' and o_orderdate < '$nextyear-01-01' and month(o_orderdate)=$month;"
		echo "Importing $year/$month into orders2."
		$MYSQLCMD -q --skip-column-names $DB -e "$sql" | /usr/local/Calpont/bin/cpimport $DB orders2 -s '\t' -n1
	done
done

# Run the queries.
for i in m*.sql; do
        echo "Running $i."
        $MYSQLCMD $DB -n -vvv -f < $i > $i.log 2>&1
        cp $i.log $dir
done

# Save the DDL/DML times off to a file.
for i in m*.sql.log; do
        grep affected $i | awk '{if ($7=="min")print substr($6,2,5)*60+$8;else print substr($6,2,5)'}
done | awk '{ttl+=$1;print $1}END{print "Total Seconds = " ttl}' > $dir/mtimes.$dt.txt

# Do a diff for the query results.
./mdiff.sh $dir baseline > mdiff.results

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
success=`grep Success mdiff.results | wc -l`
if [ $success -eq 1 ]
then
	echo "Passed (`grep DML aaa_summary.txt  | grep Total`" > status.txt
else
	echo "Failed (`grep DML aaa_summary.txt  | grep Total`" > status.txt
fi

# Output results.
echo "Script completed."
cp /var/log/Calpont/crit.log $dir
