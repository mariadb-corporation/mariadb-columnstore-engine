#!/bin/sh

#
# This script drops all tables and clears out the ssb import files from the PMs.
#
if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

if [ -z "$PGMPATH" ]; then
        PGMPATH="/usr/local/Calpont/bin"
fi

#
# Remove the import files from the PMs.
#
echo "Removing import files from PMs."
for i in `$PGMPATH/calpontConsole getsystemnet | grep pm | grep -v Console | awk '{print $6}'`; do
        for table in customer dateinfo part supplier lineorder; do
                $PGMPATH/remote_command.sh $i qalpont! "rm -f /usr/local/Calpont/data/bulk/data/import/$table.tbl" >> $TEST.log
        done
done

for database in bulkload_features dml dmlc nightly ssb ssb100c ssb100cm2 ssb100cm3 ssb1992c ssb1992q1c ssbc tpch1 tpch1c wide; do
	echo ""
	echo "Dropping tables from $database."
	for table in `$MYSQLCMD $database --skip-column-names -e "show tables;"`; do
		$MYSQLCMD $database -vvv -e "drop table $table;"
	done
done

