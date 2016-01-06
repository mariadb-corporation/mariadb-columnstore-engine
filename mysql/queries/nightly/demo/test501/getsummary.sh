#!/bin/sh
#
# Script to generate a file with the overall results.
# 

# Show query status.
echo ""
/usr/local/Calpont/bin/calpontConsole getcalponts | egrep "Build Date|Version"
echo ""
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

	if(count > 1)
	{
		x=-(1-(val[count]/val[count-1])) * 100;
		plus=x>0?"+":"";
		print "Total Query Time   :  " val[count] " seconds (" plus x "%)\n";
	}
}'

# Show query diff results.
echo ""
cat qdiff.results


# Show query times.
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

