#!/bin/bash
if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

if [ $# -ne 3 ]
then
        echo ""
        echo "Schema, tablename, and engine are required."
        echo ""
        echo "Usage:"
        echo "./createSSBTable.sh schemaName tableName engine"
        echo ""
        exit
fi

schema=$1
table=$2
engine=$3

# Validate table.
case $table in 
customer);;dateinfo);;lineorder);;part);;supplier);;
*)
echo "Table must be one of customer, dateinfo, lineorder, part, supplier."
exit
;;
esac

# Validate engine.
case $engine in 
myisam);;infinidb);;
*)
echo "Engine must be one of myisam, infinidb."
exit
esac

echo "Creating $schema.$table using engine $engine."
echo "set storage_engine=$engine;" > temp.sql
cat create_ssb_$table.sql >> temp.sql

$MYSQLCMD $schema < temp.sql

