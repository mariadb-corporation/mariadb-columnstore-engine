#!/bin/bash

#
# Runs parallel loads into customer tables.
#
# Note:
# Copy a tpch customer.tbl import file into this folder before running.
#

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

db=walt

load() {
	tableName=$1
	howMany=$2
	echo "set autocommit=0;" > $tableName.sql
	for((i=1; i<=$howMany; i++)); do
		echo "select now();" >> $tableName.sql
		echo "load data infile '/tmp/$tableName.tbl' into table $tableName fields terminated by '|';" >> $tableName.sql
	done
	echo "commit;" >> $tableName.sql

	$MYSQLCMD $db -n -vvv < $tableName.sql > $tableName.sql.log 2>&1
	if [ $? -ne 0 ]; then
		echo "`date` - Error loading table $tableName."
	else
		echo "`date` - Done loading table $tableName."
	fi
	echo "All done with $howMany loads into $tableName."
}

# Create the database and tables.
echo "Creating tpch tables in walt schema."
$MYSQLCMD -e "create database if not exists $db;"
$MYSQLCMD $db -n -vvv < ../../../../scripts/create_tpch_4byte.sql > create.sql.log 2>&1

# Copy the .tbl files to the /tmp directory.
for tbl in region nation customer supplier part partsupp orders lineitem; do
	echo "Creating /tmp/$tbl.tbl."
	cp /usr/local/Calpont/data/bulk/data/import_local/tpch/1g/$tbl.tbl /tmp/.
done

load region 100 &
load nation 100 &
load customer 20 &
load part 5 &
load supplier 100 &
load partsupp 8 &
load orders 20 &
load lineitem 1 &

wait

for tbl in region nation customer supplier part partsupp orders lineitem; do
	echo "Removing /tmp/$tbl.tbl."
done

echo ""
echo "All done."
