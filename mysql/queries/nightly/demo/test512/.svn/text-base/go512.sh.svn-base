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
echo "Running group by query at 4GB TotalUmMemory limit several times.  Takes a while."
$MYSQLCMD $DB -f -n -vvv < groupby.sql > groupby.sql.log 2>&1

#
# Run a diff report.
#
egrep -v "MaxMem|count|getstats|sec|--|Bye" groupby.sql.log | grep "|" > diff.log
egrep -v "MaxMem|count|getstats|sec|--|Bye" groupby.sql.ref.log | grep "|" > diff.ref.log
diff diff.log diff.ref.log > diff.txt 2>&1

