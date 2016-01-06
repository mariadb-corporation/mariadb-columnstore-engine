#!/bin/bash

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

echo "Creating tpch1tc, nightly100, and ssb100c databases."
$MYSQLCMD -vvv -e "create database if not exists tpch1tc; create database if not exists nightly100; create database if not exists ssb100c;"

echo "Dropping and creating tpch1tc tables."
$MYSQLCMD tpch1tc -vvv < /root/genii/mysql/scripts/create_tpch_8byte.sql 

echo "Dropping and creating ssb100c tables."
$MYSQLCMD ssb100c -vvv < /root/genii/mysql/scripts/create_ssb_schema.sql 

echo ""
echo "Dropping and creating nightly100.orders table."
$MYSQLCMD nightly100 -vvv < /root/genii/mysql/scripts/create_tpch_4byte.sql
$MYSQLCMD nightly100 -vvv -e "drop table lineitem; drop table nation; drop table region; drop table supplier; drop table customer; drop table part; drop table partsupp;"




