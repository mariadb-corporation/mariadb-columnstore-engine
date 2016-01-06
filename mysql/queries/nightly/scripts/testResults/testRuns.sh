#!/bin/bash

#
# This script will list the results for the given test name across all of the servers in the for loop.
#

usage()
{
	echo "Usage:"
	echo "	${0##*/} testName" >&2
	echo "	${0##*/} testName userModule" >&2
	echo "" >&2
	echo "Examples:" >&2
	echo "	./testResults.sh test000" >&2
	echo "	./testResults.sh test000 srvswdev11" >&2
	echo ""
	echo "Note:  Use "summary" as the testName to report a summary report for all tests."
 	exit 1	
}

getSummarySql()
{
	summarySql="
		select  rpad('$userModule', 12, ' ') UM,
			r.runId Run,
	       		r.version Ver,
		        r.rel Rel,
			buildDtm Built,
		        r.start Start,
	        	timediff(r.stop, r.start) 'Run Time',
		        count(tr.runid) 'Total',
		       	if(sum(if(locate('Failed', tr.status)>0,1,0))>0,'Failed', 'Passed') Status,
	        	ifnull(group_concat(if(locate('Failed', tr.status)>0,substr(test,5,4),null)order by test),'') 'Failed Tests'
		from run r
		join testrun tr on r.runid=tr.runid
		group by 1, 2, 3, 4, 5
		order by 1;"
}

if [ "$1" == "-h" ] || [ $# -lt 1 ]; then
	usage
	exit 1
fi

DB=testresults
test=$1
			
echo ""
if [ $# -eq 1 ]; then
	for userModule in qaftest2 srvswdev11 srvperf3 srvperf2 srvalpha2 srvprodtest1 qaftest7; do
		echo "$userModule:"
		if [ "$test" == "summary" ]; then
			getSummarySql
			/usr/bin/mysql -h $userModule -u root --database=$DB --table -e "$summarySql"
		else
			/usr/bin/mysql -h $userModule -u root --database=$DB --table -e "select rpad('$userModule', 12, ' ') UM, start Start, timediff(stop, start) 'run time', status from testrun where test='$test';"
		fi
		echo ""
	done
else
	userModule=$2
	echo "$userModule:"
	getSummarySql
	if [ "$test" == "summary" ]; then
		/usr/bin/mysql -h $userModule -u root --database=$DB --table -e "$summarySql"
	else
		sql="
			select 	r.runId Id,
				r.version Ver,
				r.rel Rel,
				tr.start Start, 
				timediff(tr.stop, tr.start) 'Run Time', 
				replace(tr.status, '   ', '') Status
			from testrun tr join run r using (runId)
			where tr.test='$test';
		"
		
		/usr/bin/mysql -h $userModule -u root --database=$DB --table -e "$sql"
	fi
fi

