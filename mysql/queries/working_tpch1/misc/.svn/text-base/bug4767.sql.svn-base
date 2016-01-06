create table if not exists bug4767 (c1 float, c2 double)engine=infinidb;
insert into bug4767 values (1.234, 3.4556), (2.3345456, 2.3345456);
select * from bug4767 a, bug4767 b where a.c1 = b.c1;
select * from bug4767 a join bug4767 b using (c2);
select * from bug4767 a join bug4767 b on (a.c1=b.c2);
select count(*) from datatypetestm a, datatypetestm b where b.cfloat = a.cfloat;
select count(*) from datatypetestm a, datatypetestm b where b.cdouble = a.cinteger;
drop table bug4767;

