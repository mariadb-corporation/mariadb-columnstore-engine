# create supplier table
#
--disable_warnings
drop table if exists supplier;
--enable_warnings
create table supplier (
        s_suppkey int,
        s_name char (25),
        s_address varchar (25),
        s_city char (10),
        s_nation char (15),
        s_region char (12),
        s_phone char (15)
) engine=Columnstore;
SHOW CREATE TABLE supplier;
