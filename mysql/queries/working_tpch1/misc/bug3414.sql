drop table if exists qa_cast;
create table qa_cast ( s1 varchar(20), qaint1 int, qadouble double) engine=infinidb;
insert into qa_cast values ('123456789',123456789, 1234);
insert into qa_cast values ('123.45',123456789, 123.45);
insert into qa_cast values ('abc',123456789, 1.5);
insert into qa_cast values ('0.001',123456789, 0.001);
select s1, qaint1, mod(s1, 10), mod(qaint1, 10), mod(qadouble, 10)  from qa_cast;
drop table qa_cast;



