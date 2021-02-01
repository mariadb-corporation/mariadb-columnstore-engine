# create lineorder table
#
--disable_warnings
drop table if exists lineorder;
--enable_warnings
create table lineorder (
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
) engine=Columnstore;
SHOW CREATE TABLE lineorder;
