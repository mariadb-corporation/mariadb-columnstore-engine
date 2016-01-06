#!/bin/bash

DB=wide
TBL1=bug3515
TBL2=bug3515b

echo "1) Creating tables."
$MYSQLCMD -e "create database if not exists $DB;" > create.log 2>&1
$MYSQLCMD $DB -e "drop table if exists $TBL1; drop table if exists $TBL2;" >> create.log 2>&1
$MYSQLCMD $DB -e "create table $TBL1(c1 smallint)engine=infinidb; create table $TBL2(c1 int)engine=infinidb;" >> create.log 2>&1

echo "2) Populating $TBL1 table with 20 million rows."
echo "dummy" | awk '{for(i=1; i<=20000000; i++)print 1}' | $CPIMPORTCMD $DB $TBL1 >> create.log 2>&1

echo "3) Deleting the 20 million rows."
$MYSQLCMD $DB -e "delete from $TBL1;" >> create.log 2>&1

echo "4) Dropping $TBL2."
$MYSQLCMD $DB -e "drop table $TBL2;" >> create.log 2>&1

# A new extent will get added picking up the LBIDs from the dropped table above.  The last extent will not be the last one in the extent map
# which exposes the bug.  
echo "5) Populating $TBL1 table with 10 million more rows."
        echo "dummy" | awk -v num=$i '{for(i=1; i<=10; i++)for(j=1; j<=1000000; j++)print i}' | $CPIMPORTCMD $DB $TBL1 >> create.log 2>&1

echo "6) Creating $TBL2 again."
$MYSQLCMD $DB -e "create table $TBL2(c1 int)engine=infinidb; insert into $TBL2 values (1), (2);"

