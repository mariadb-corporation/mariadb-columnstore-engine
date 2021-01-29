# create orders table
#
--disable_warnings
drop table if exists orders;
--enable_warnings
create table orders (
        o_orderkey int,
        o_custkey int,
        o_orderstatus char (1),
        o_totalprice decimal(12,2),
        o_orderdate date,
        o_orderpriority char (15),
        o_clerk char (15),
        o_shippriority int,
        o_comment varchar (79)
) engine=columnstore;
SHOW CREATE TABLE orders;
