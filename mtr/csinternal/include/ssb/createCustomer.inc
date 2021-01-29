# create customer table
#
--disable_warnings
drop table if exists customer;
--enable_warnings
create table customer (
        c_custkey int,
        c_name varchar (25),
        c_address varchar (25),
        c_city char (10),
        c_nation char (15),
        c_region char (12),
        c_phone char (15),
        c_mktsegment char (10)
) engine=Columnstore;
SHOW CREATE TABLE customer;
