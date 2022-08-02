# create supplier table
#
--disable_warnings
drop table if exists supplier;
--enable_warnings
create table supplier (
        s_suppkey int,
        s_name char (25),
        s_address varchar (40),
        s_nationkey int,
        s_phone char (15),
        s_acctbal decimal(12,2),
        s_comment varchar (101)
) engine=columnstore;
SHOW CREATE TABLE supplier;
