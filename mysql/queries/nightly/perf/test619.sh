#!/bin/sh
#
# Test 619.  Load data infile of 2 million lineorder rows.
#
TEST=test619
STATUSTEXT="619 Load Data Infile 2M Lineorder Rows : "
DB=ssb100c

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
        export MYSQLCMD
fi

if [ -z "$PGMPATH" ]; then
	PGMPATH="/usr/local/Calpont/bin"
fi

if [ -z "$DATASOURCEPATH" ]; then
	DATASOURCEPATH="/root/ssb"
fi

echo "$STATUSTEXT In Progress" > $TEST.status

#
# Create the load file.
#
head -2000000 $DATASOURCEPATH/lineorder.tbl > /tmp/lineorder2.tbl

#
# Create the lineorder2 table.
#
sql="
drop table if exists lineorder2;
create table lineorder2 (
        lo_orderkey bigint,
        lo_linenumber int,
        lo_custkey int,
        lo_partkey int,
        lo_suppkey int,
        lo_orderdate int,
        lo_orderpriority char (15),
        lo_shippriority char (1),
        lo_quantity decimal (12,2),
        lo_extendedprice decimal (12,2),
        lo_ordtotalprice decimal (12,2),
        lo_discount decimal (12,2),
        lo_revenue decimal (12,2),
        lo_supplycost decimal (12,2),
        lo_tax decimal (12,2),
        lo_commitdate int,
        lo_shipmode char (10)
) engine=infinidb
;
"
$MYSQLCMD $DB -vvv -e "$sql" > $TEST.log 2>&1

#
# Load the rows.
#
sec1=`date '+%s.%N'`
$MYSQLCMD $DB -vvv -e "load data infile '/tmp/lineorder2.tbl' into table lineorder2 fields terminated by '|';" >> $TEST.log 2>&1
sec2=`date '+%s.%N'`
rowRate=`$MYSQLCMD $DB --skip-column-names -e "select format(2000000/($sec2-$sec1), 0);"`

#
# Populate the .status file.
#
errorCount=`grep -i error $TEST.log | wc -l`

if [ $errorCount -gt 0 ]
then
        echo "$STATUSTEXT Failed (Check $TEST.log for errors)" > $TEST.status
else
        echo "$STATUSTEXT Passed ($rowRate rows per second)" > $TEST.status
fi

cat $TEST.status
