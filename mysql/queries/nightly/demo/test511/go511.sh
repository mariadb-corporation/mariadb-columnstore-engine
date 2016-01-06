#!/bin/sh
DB=`../getDatabaseName.sh tpch1tc`

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

echo "Test still running." > diff.txt
#
# Run the union script.
#
echo "Running UM unions at 4GB TotalUmMemory limit.  Takes a while."
$MYSQLCMD $DB -f -n -vvv < unions.sql > unions.sql.log 2>&1

#
# Run a diff report.
#
egrep -v "MaxMem|count|getstats|sec|--|Bye" unions.sql.log | grep "|" > diff.log
egrep -v "MaxMem|count|getstats|sec|--|Bye" unions.sql.ref.log | grep "|" > diff.ref.log
diff diff.log diff.ref.log > diff.txt 2>&1

