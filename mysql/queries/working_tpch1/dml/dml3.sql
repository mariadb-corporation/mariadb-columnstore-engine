drop table if exists qa_45678;
create table qa_45678 (c1 char(4),c2 char(4)) engine=infinidb;
insert into qa_45678 (c1,c2) values ('1234','1234');
insert into qa_45678 (c1,c2) values (NULL,NULL);
insert into qa_45678 (c1,c2) values ('aaaa','bbbb');
insert into qa_45678 (c1,c2) values ('AAAA','BBBB');
select * from qa_45678;
drop table qa_45678;
