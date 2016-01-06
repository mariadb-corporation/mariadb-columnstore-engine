#!/bin/bash

if [ $# -ne 1 ]; then
	echo ""
	echo "Please pass the sql file to compare as a parameter."
	echo ""
	echo "Example:"
	echo "./diff.sh q0101.sql"
	echo ""
	exit 1
fi

fl=$1

awk -F '|' '
{
	if(index($0, "--") > 0) {
		started=1;
	}
	else if(started==1 && index($0,"rows") < 1) {
		for(i=1; i<=NF; i++) {
			val=$i; 
			gsub(/[[:space:]]*/, "", val);
			if(i < NF) {
				printf val "|";
			}
			else {
				print val "|";
			}
		}
	}
}
' $fl.ref.log > 1.txt

awk '
{
	if(NR > 1) {
		for(i=1; i<=NF; i++) {
			val=$i; 
			gsub(/[[:space:]]*/, "", val);
			if(index(val, "NULL") > 0) {
				val="";
			}
			if(i < NF) {
				printf val "|";
			}
			else {
				print val "|";
			}
		}
	}
}
' $fl.log > 2.txt

# Quick hack to show not compared when there is an error in the .sql.log file.  
if [ `grep -i error $fl.log | wc -l | awk '{print $1}'` -gt 0 ]; then
	cat $fl.log > 2.txt
fi

diff -w 1.txt 2.txt -b > /dev/null

if [ $? -eq 0 ]; then
	echo ""
	echo "It's a match!"
	echo ""
	exit 0
else
	echo ""
	echo "Results do not match."
	echo ""
	echo "sdiff -b 1.txt 2.txt:" 
	sdiff -b 1.txt 2.txt
	echo ""
	echo "Ref:"
	cat $fl.ref.log
	echo ""
	echo "Log:"
	cat $fl.log
	echo ""
	exit 1
fi

