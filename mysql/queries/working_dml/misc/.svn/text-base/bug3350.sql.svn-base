drop table if exists bug3350;
create table bug3350 ( a bigint, b varchar(32), c varchar(4),  d double, e tinyint, f float, g decimal(8,2), h decimal (10,4), i int, j char(6), k smallint ) engine=infinidb;
insert into bug3350 values( 1, 'column b1', 'cor1', 12345.67, 0, 1234.56, 123.45, 1234.56, 1, 'charc1', 1);
insert into bug3350 values( null, 'column b2', 'cor2', 1.797693231E+108, -10, 0, 9.98, 99999999999.98, null, 'charc2', 2);
insert into bug3350 values( 55555555000000, 'column b3', 'cor3', null, 1, 3.402, 100.12, 0, 7483645, 'charc3', 3);
insert into bug3350 values( 72036854775806, 'column b4', 'cor4', 0, null, 1234.56, null, 1234.56, 0, 'charc4', 765);
insert into bug3350 values( 72036854775804, 'column b5', 'cor5', 1.797693231E+108, 128, 0, 123.45, null, 7483647, 'charc6', null);
insert into bug3350 values( 0, 'column b6', 'cor6', 12345.67, 12, null, 9.97, 99999999999.98, 1000, 'char6c', 767);
select * from bug3350;
set autocommit=0;
-- from double to others
update bug3350 set a=d, e=d, g=d, h=d, i=d, k=d;
select * from bug3350;
rollback;
-- from float to others
update bug3350 set a=f, e=f, g=f, h=f, i=f, k=f;
select * from bug3350;
rollback;
-- from decimal to decimal
update bug3350 set g=h;
select * from bug3350;
rollback;
-- from smallint to tinyint
update bug3350 set e=h;
select * from bug3350;
rollback;
-- from varchar to char
update bug3350 set j=c;
select * from bug3350;
rollback;
-- from double to float
update bug3350 set f=d;
select * from bug3350;
rollback;
-- from bigint to others
update bug3350 set d=a, b=a, e=a, f=a, g=a, i=a, h=a, k=a;
select * from bug3350;
rollback;
select * from bug3350;
drop table bug3350;

