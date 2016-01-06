#!/bin/bash

#
# Lists the nightly test runs on the local server.
#

#
# If a parameter was passed, only show tests matching the testFolder.
#
if [ -z "$1" ]; then
	testFolder="%"
else
	testFolder=$1
fi

if [ -z "$MYSQLCMD" ]; then
	MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

sql="set max_length_for_sort_data=1000000;
select 	r.runid id, 
	r.version ver, 
	r.rel, 
	r.start, 
	r.buildDtm, 
	r.testFolder, 
	timediff(r.stop, r.start) tm,
	count(tr.runid) run, 
--	group_concat(tr2.test) Failed
	ifnull(group_concat(substr(tr2.test,5,8) order by 1),'') Failed
from 	run r 
	join testrun tr on r.runid = tr.runid
	left join testrun tr2 on tr.runid = tr2.runid and tr.test = tr2.test and tr2.status like '%Failed%'
where testFolder like '$testFolder'
group by 1,2,3,4,5,6,7
order by 1;"

$MYSQLCMD testResults -vvv -e "$sql;"
