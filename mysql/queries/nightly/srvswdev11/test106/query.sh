#!/bin/bash

if [ $# -lt 3 ]; then
	echo "query.sh database queryID rowsPerLoad - three parms required."
	exit
fi

DB=$1
ID=$2
ROWSPERLOAD=$3

QUERY=query$ID
ITERATION=0
QUERYLOGTABLE=test006_query

# Loop until the go script is done
rm -f logs/$QUERY.sql.log*

stillGoing=1
while [ $stillGoing -gt 0 ]
do
	let ITERATION++
	$MYSQLCMD $DB -e "insert into $QUERYLOGTABLE values ($ID, $ITERATION, now(), null, null, null, null);"
	SUFFIX=`printf "%06i" $ITERATION`
        $MYSQLCMD $DB -vvv -n < $QUERY.sql > logs/$QUERY.sql.log.$SUFFIX 2>&1
	$MYSQLCMD $DB -e "update $QUERYLOGTABLE set stop=now() where id=$ID and iteration=$ITERATION;"
	errs=`grep -i ERROR logs/$QUERY.sql.log.$SUFFIX | wc -l`
	rows=`grep query logs/$QUERY.sql.log.$SUFFIX | grep -v select | wc -l`
	incorrect=`grep query logs/$QUERY.sql.log.$SUFFIX | grep -v batch | awk -v rpl=$ROWSPERLOAD '{if($9 != 1 || $11 != rpl || $13 != rpl || $15 != rpl || $17 != rpl || $19 != rpl || $21 != rpl)print $0}' | wc -l`
	
	$MYSQLCMD $DB -e "update $QUERYLOGTABLE set err=$errs, rowsReturned=$rows, correctResults=($incorrect <= 0) where id=$ID and iteration=$ITERATION;"
	
	if [ $incorrect -eq 0 ] && [ $errs -eq 0 ]
	then
		rm -f logs/$QUERY.sql.log.$SUFFIX
	else
		sleep 1
	fi

        if [ -f stop.txt ]; then
                echo "Found stop.txt.  Query $ID script stopping."
                exit
        fi
done

