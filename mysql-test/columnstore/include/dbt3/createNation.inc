# create nation table
#
--disable_warnings
drop table if exists nation;
--enable_warnings
create table nation (
        n_nationkey int,
        n_name char (25),
        n_regionkey int,
        n_comment varchar (152)
) engine=columnstore;
SHOW CREATE TABLE nation;
