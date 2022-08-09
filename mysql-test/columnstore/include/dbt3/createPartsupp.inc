# create partsupp table
#
--disable_warnings
drop table if exists partsupp;
--enable_warnings
create table partsupp (
        ps_partkey int,
        ps_suppkey int,
        ps_availqty int,
        ps_supplycost decimal(12,2),
        ps_comment varchar (199)
) engine=columnstore;
SHOW CREATE TABLE partsupp;
