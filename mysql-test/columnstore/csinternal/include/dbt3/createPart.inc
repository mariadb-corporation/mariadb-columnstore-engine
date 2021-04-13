# create part table
#
--disable_warnings
drop table if exists part;
--enable_warnings
create table part (
        p_partkey int,
        p_name varchar (55),
        p_mfgr char (25),
        p_brand char (10),
        p_type varchar (25),
        p_size int,
        p_container char (10),
        p_retailprice decimal(12,2),
        p_comment varchar (23)
) engine=columnstore;
SHOW CREATE TABLE part;
