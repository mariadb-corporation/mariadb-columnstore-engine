#!/bin/bash

session=1
DB=dml

if [ -n "$1" ] ; then
        session="$1"
fi

if [ -n "$2" ] ; then
	DB=$2
fi

# Loop until the go7 script is done
rm -f counts.sql.log.$session
touch counts.sql.log.$session

while /bin/true; do
        if [ -f stop.txt ]; then
                echo "Found stop.txt.  Query script $session stopping."
                exit
        fi
	$MYSQLCMD $DB -n < counts.sql >> counts.sql.log.$session 2>&1
done
