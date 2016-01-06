#!/bin/sh
DB=`../getDatabaseName.sh ssb100c`

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

#
# Run the query script.
#
echo "Running script grouping by c_name of increasing width."
$MYSQLCMD $DB -f -n -vvv < groupByWiderAndWider.sql > groupByWiderAndWider.sql.log 2>&1

#
# Validate that all 20 queries completed (10 queries + 10 calls to calgetstats).
#
errs=`grep -i error groupByWiderAndWider.sql.log | wc -l`
queries=`grep sec groupByWiderAndWider.sql.log | wc -l`

if [ $errs -gt 0 ]
then
        echo "Failed (Errors - check test520/groupByWiderAndWider.sql.log)" > status.txt
elif [ $queries -ne 20 ]
then
        echo "Failed (Missing query results - check test520/groupByWiderAndWider.sql.log)" > status.txt
else
        echo "Passed" > status.txt
fi
