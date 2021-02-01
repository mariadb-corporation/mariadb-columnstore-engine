# create customer table
#
--disable_warnings
drop table if exists customer;
--enable_warnings
create table customer (
        c_custkey int,
        c_name varchar (25),
        c_address varchar (40),
        c_nationkey int,
        c_phone char (15),
        c_acctbal decimal(12,2),
        c_mktsegment char (10),
        c_comment varchar (117)
) engine=columnstore;
SHOW CREATE TABLE customer;
