# create region table
#
--disable_warnings
drop table if exists region;
--enable_warnings
create table region (
        r_regionkey int,
        r_name char (25),
        r_comment varchar (152)
) engine=columnstore;
SHOW CREATE TABLE region;
