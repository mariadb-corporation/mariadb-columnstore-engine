#!/bin/sh
#
# Script to generate a file with the overall results.
# 

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

	if(count > 1)
	{
		if(val[count-1] > 0)
			x=-(1-(val[count]/val[count-1])) * 100;
		else
			x=0;
		plus=x>0?"+":"";
		print "Total DDL/DML Time :  " val[count] " seconds (" plus x "%)";
	}
}'

# Show DML/DDL diff results.
cat mdiff.results
echo ""

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
