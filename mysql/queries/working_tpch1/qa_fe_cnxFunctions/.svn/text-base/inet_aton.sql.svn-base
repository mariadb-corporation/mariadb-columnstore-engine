# drop (if necessary) and recreate the table
drop table if exists inet_aton_test;
create table inet_aton_test (
    id1      int,
    id2      int,
    ipstring char(20),
    ipnum    bigint,
    dbl      double,
    flt      float,
    str      char(20),
    d        decimal(10,3)
    ) engine=infinidb;

# populate the table
insert into inet_aton_test values (0, 1, '209.207.224.40', 3520061480, 209.207, 209.207, '209.207.224.40', 209.207);
insert into inet_aton_test values (1, 1, '209.207.224.41', 3520061481, 209.207, 209.207, '209.207.224.41', 209.207);
insert into inet_aton_test values (1, 2, '209.207.224.41', 3520061481, 209.207, 209.207, '209.207.224.41', 209.207);
insert into inet_aton_test values (2, 1, '209.207.224.42', 3520061482, 209.207, 209.207, '209.207.224.42', 209.207);
insert into inet_aton_test values (2, 2, '209.207.224.42', 3520061482, 209.207, 209.207, '209.207.224.42', 209.207);
insert into inet_aton_test values (2, 3, '209.207.224.42', 3520061482, 209.207, 209.207, '209.207.224.42', 209.207);
insert into inet_aton_test values (3, 1, '209.207.224.43', 3520061483, 209.207, 209.207, '209.207.224.43', 209.207);
insert into inet_aton_test values (3, 2, '209.207.224.43', 3520061483, 209.207, 209.207, '209.207.224.43', 209.207);
insert into inet_aton_test values (3, 3, '209.207.224.43', 3520061483, 209.207, 209.207, '209.207.224.43', 209.207);
insert into inet_aton_test values (3, 4, '209.207.224.43', 3520061483, 209.207, 209.207, '209.207.224.43', 209.207);
insert into inet_aton_test values (4, 1, '209.207.224.44', 3520061484, 209.207, 209.207, '209.207.224.44', 209.207);
insert into inet_aton_test values (4, 2, '209.207.224.44', 3520061484, 209.207, 209.207, '209.207.224.44', 209.207);
insert into inet_aton_test values (4, 3, '209.207.224.44', 3520061484, 209.207, 209.207, '209.207.224.44', 209.207);
insert into inet_aton_test values (4, 4, '209.207.224.44', 3520061484, 209.207, 209.207, '209.207.224.44', 209.207);
insert into inet_aton_test values (4, 5, '209.207.224.44', 3520061484, 209.207, 209.207, '209.207.224.44', 209.207);
insert into inet_aton_test values (5, 1, '127',            127,        127.0,   127.0,   '0.0.0.127',      127.000);
insert into inet_aton_test values (6, 1, '127.1',          2130706433, 127.1,   127.1,   '127.0.0.1',      127.100);
insert into inet_aton_test values (7, 1, '127.2.1',        2130837505, 127.2,   127.2,   '127.2.0.1',      127.200);

# all these queries return 1 row
select * from inet_aton_test where inet_aton(ipstring) = 3520061481 and id2 = 1;
select * from inet_aton_test where inet_aton(ipstring) = '3520061482' and id2 = 1;
select * from inet_aton_test where inet_aton(ipstring) = 3520061483. and id2 = 1;

# these queries return 18 and 8 rows respectively
select id1, id2, ipstring, inet_aton(ipstring) from inet_aton_test order by 3 desc,2 asc;
select inet_aton(ipstring), count(*) from inet_aton_test group by 1 order by 1 asc;

# each of these queries return 1 row
select * from inet_aton_test where inet_aton(ipstring) = 127;
select * from inet_aton_test where inet_aton(ipstring) = 2130706433;
select * from inet_aton_test where inet_aton(ipstring) = 2130837505;

drop table inet_aton_test;
