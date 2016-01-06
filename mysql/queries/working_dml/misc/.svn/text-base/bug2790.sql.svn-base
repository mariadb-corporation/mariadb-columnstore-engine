drop table if exists bug2790;
create table bug2790 ( a bigint, b varchar(32), c date, d varchar(32), e varchar(32) ) engine=infinidb;
# Expected result of the load data infile is:
# ERROR 122 (HY000) at line 3: The syntax '\0' is not supported by InfiniDB.
load data infile '/tmp/bug2790.tbl' into table bug2790;
drop table bug2790;

