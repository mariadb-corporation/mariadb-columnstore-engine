# drop (if necessary) and recreate the table
drop table if exists inet_ntoa_test;
create table inet_ntoa_test (
    id1      int,
    id2      int,
    ipstring char(20),
    ipnum    bigint,
    dbl      double,
    flt      float,
    dc       decimal(10,3)
    ) engine=infinidb;

# populate the table
insert into inet_ntoa_test values (0, 1, '209.207.224.40', 3520061480, NULL, NULL, NULL);
insert into inet_ntoa_test values (1, 1, '209.207.224.41', 3520061481, NULL, NULL, NULL);
insert into inet_ntoa_test values (1, 2, '209.207.224.41', 3520061481, NULL, NULL, NULL);
insert into inet_ntoa_test values (2, 1, '209.207.224.42', 3520061482, NULL, NULL, NULL);
insert into inet_ntoa_test values (2, 2, '209.207.224.42', 3520061482, NULL, NULL, NULL);
insert into inet_ntoa_test values (2, 3, '209.207.224.42', 3520061482, NULL, NULL, NULL);
insert into inet_ntoa_test values (3, 1, '209.207.224.43', 3520061483, NULL, NULL, NULL);
insert into inet_ntoa_test values (3, 2, '209.207.224.43', 3520061483, NULL, NULL, NULL);
insert into inet_ntoa_test values (3, 3, '209.207.224.43', 3520061483, NULL, NULL, NULL);
insert into inet_ntoa_test values (3, 4, '209.207.224.43', 3520061483, NULL, NULL, NULL);
insert into inet_ntoa_test values (4, 1, '209.207.224.44', 3520061484, NULL, NULL, NULL);
insert into inet_ntoa_test values (4, 2, '209.207.224.44', 3520061484, NULL, NULL, NULL);
insert into inet_ntoa_test values (4, 3, '209.207.224.44', 3520061484, NULL, NULL, NULL);
insert into inet_ntoa_test values (4, 4, '209.207.224.44', 3520061484, NULL, NULL, NULL);
insert into inet_ntoa_test values (4, 5, '209.207.224.44', 3520061484, NULL, NULL, NULL);
insert into inet_ntoa_test values (5, 1, '0.0.0.127',      127,        NULL, NULL, NULL);
insert into inet_ntoa_test values (6, 1, '127.0.0.1',      2130706433, NULL, NULL, NULL);
insert into inet_ntoa_test values (7, 1, '127.2.0.1',      2130837505, NULL, NULL, NULL);
insert into inet_ntoa_test values (8, 1, NULL,        -72036854775806, NULL, NULL, NULL);
insert into inet_ntoa_test values (8, 2, NULL,         72036854775803, NULL, NULL, NULL);
insert into inet_ntoa_test values (9, 1, NULL,             NULL,       NULL, NULL, -999);
insert into inet_ntoa_test values (9, 2, NULL,             NULL,       NULL, NULL, -995);
insert into inet_ntoa_test values (9, 3, '0.0.3.227',      995,        NULL, NULL, 995 );
insert into inet_ntoa_test values (9, 4, '0.0.3.231',      999,        NULL, NULL, 999 );
insert into inet_ntoa_test values (10,5, NULL,             NULL,       NULL, NULL, -9.990);
insert into inet_ntoa_test values (10,6, NULL,             NULL,       NULL, NULL, -9.950);
insert into inet_ntoa_test values (10,7, '0.0.0.10',       10,         NULL, NULL, 9.950 );
insert into inet_ntoa_test values (10,8, '0.0.0.10',       10,         NULL, NULL, 9.990 );
insert into inet_ntoa_test values (11,1, '0.0.0.0',        0,          0.0,  0.0,  0.000 );

# all these queries return 1 row
select * from inet_ntoa_test where inet_ntoa(ipnum) = '209.207.224.41' and id2 = 1;
select * from inet_ntoa_test where inet_ntoa(ipnum) = '209.207.224.42' and id2 = 1;
select * from inet_ntoa_test where inet_ntoa(ipnum) = '209.207.224.43' and id2 = 1;

# these queries return 18 and 8 rows respectively
select id1, id2, ipstring, inet_ntoa(ipnum) from inet_ntoa_test where id1 < 8 order by 3 desc,2 asc;
select inet_ntoa(ipnum), count(*) from inet_ntoa_test where id1 < 8 group by 1 order by 1 asc;

# each of these queries return 1 row
select * from inet_ntoa_test where inet_ntoa(ipnum) = '0.0.0.127';
select * from inet_ntoa_test where inet_ntoa(ipnum) = '127.0.0.1';
select * from inet_ntoa_test where inet_ntoa(ipnum) = '127.2.0.1';

# query returns 2 rows and reports the ip number as NULL (out of range)
select id1, id2, dc, inet_ntoa(ipnum) from inet_ntoa_test where id1 = 8 order by id2;

# query returns 4 rows and reports the ip number as:
#   NULL for the first 2 rows (tests out of range)
#   0.0.3.227, and 0.0.3.231 for the 3rd and 4th rows
select id1, id2, ipstring, dc, inet_ntoa(dc) from inet_ntoa_test where id1 = 9 order by id2;

# query returns 4 rows and reports the ip number as:
#   NULL for the first 2 rows (tests out of range)
#   0.0.0.10 for the 3rd and 4th rows (tests rounding up)
select id1, id2, dc, inet_ntoa(dc) from inet_ntoa_test where id1 = 10 order by id2;

# query returns 1 row (tests ip number 0)
select id1, id2, dc, inet_ntoa(dc) from inet_ntoa_test where id1 = 11;

#drop table inet_ntoa_test;
