#!/bin/sh
#
# Script to generate a file with the overall results.
# 

# Show query status.
# Green:  No slower than 1% of previous time.
# Yellow: 1 to 5% slower than previous time.
# Red:    Over 5% slower than previous time.

grep -r "Total Seconds = " 20*/qtim* | 
awk '
{
	count++;
	val[count]=$4;
}END{
	if(count > 1)
		factor=val[count]/val[count-1];
	else
		factor = 1;
	if(factor>1.05)
	{
		print "Query Status:  Red";
	}
	else if(factor>1.01)
	{
		print "Query Status:  Yellow";
	}
	else if(factor>.98)
	{
		print "Query Status:  Green";
	}
	else
	{
		print "Query Status:  Super Green";
	}

	if(count > 1)
	{
		x=-(1-(val[count]/val[count-1])) * 100;
		plus=x>0?"+":"";
		print "Query Time % change:  " plus x "%";
	}
}'

# Show DDL/DML status.
grep -r "Total Seconds = " 20*/mtim* |  
awk '
{
	count++;
	val[count]=$4;
}END{
	if(count > 1 && val[count-1] > 0)
		factor=val[count]/val[count-1];
	else
		factor = 1;
	if(factor>1.05)
	{
		print "DDL/DML Status:  Red";
	}
	else if(factor>1.01)
	{
		print "DDL/DML Status:  Yellow";
	}
	else if(factor>.98)
	{
		print "DDL/DML Status:  Green";
	}
	else
	{
		print "DDL/DML Status:  Super Green";
	}

	if(count > 1)
	{
		if(val[count-1] > 0)
			x=-(1-(val[count]/val[count-1])) * 100;
		else
			x=0;
		plus=x>0?"+":"";
		print "DDL/DML Time % change:  " plus x "%";
	}
}'

# Show query times.
echo "";
echo "Query Time In Seconds";
echo "---------------------------------------";
grep -r "Total Seconds = " 20*/qtim* |  
awk '
{
	count++;
	val[count]=$4;
	if(count==1)
	{
		print $1 " " $4;
	}
	else
	{
		if(val[count-1] > 0)
			x=-(1-(val[count]/val[count-1])) * 100;
		else
			x=0;
		plus=x>0?"+":"";
		print $1 " " $4 " (" plus x "%)";
		
	}
}'

# Show Blocks Touched.
echo "";
echo "Blocks Touched";
echo "---------------------------------------";
grep -r "Total Block" 20*/blo* |  
awk '
{
	count++;
	val[count]=$5;
	if(count==1)
	{
		print $1 " " $5;
	}
	else
	{
		if(val[count-1] > 0)
			x=-(1-(val[count]/val[count-1])) * 100;
		else
			x=0;
		plus=x>0?"+":"";
		print $1 " " $5 " (" plus x "%)";
	}
}'

# Show Physical I/O.
echo "";
echo "Blocks Read From Disk";
echo "---------------------------------------";
grep -r "Total Phy" 20*/phy* |  
awk '
{
	count++;
	val[count]=$6;
	if(count==1)
	{
		print $1 " " $6;
	}
	else
	{
		if(val[count-1] > 0)
			x=-(1-(val[count]/val[count-1])) * 100;
		else
			x=0;
		plus=x>0?"+":"";
		print $1 " " $6 " (" plus x "%)";
	}
}'

# CP Blocks Eliminated 
echo "";
echo "Casual Partitioning Blocks Eliminated";
echo "---------------------------------------";
grep -r "Total CP" 20*/cpb* |  
awk '
{
	count++;
	val[count]=$6;
	if(count==1)
	{
		print $1 " " $6;
	}
	else
	{
		if(val[count-1]>0)
			x=-(1-(val[count]/val[count-1])) * 100;
		else
			x=0;
		plus=x>0?"+":"";
		print $1 " " $6 " (" plus x "%)";
	}
}'

# Show DML/DDL times.
echo "";
echo "DDL/DML Time In Seconds";
echo "---------------------------------------";
grep -r "Total Seconds = " 20*/mtim* |  
awk '
{
	count++;
	val[count]=$4;
	if(count==1)
	{
		print $1 " " $4;
	}
	else
	{
		if(val[count-1] > 0)
			x=-(1-(val[count]/val[count-1])) * 100;
		else
			x=0;
		plus=x>0?"+":"";
		print $1 " " $4 " (" plus x "%)";
		
	}
}'
