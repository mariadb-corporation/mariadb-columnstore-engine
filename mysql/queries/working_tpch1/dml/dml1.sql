drop table if exists qa_4567890123456789012345;
create table qa_4567890123456789012345 (c1 char(4),c2 char(4)) engine=infinidb;
insert into qa_4567890123456789012345 (c1,c2) values ('1234','1234');
insert into qa_4567890123456789012345 (c1,c2) values (NULL,NULL);
insert into qa_4567890123456789012345 (c1,c2) values ('aaaa','bbbb');
insert into qa_4567890123456789012345 (c1,c2) values ('AAAA','BBBB');
select * from qa_4567890123456789012345;
drop table qa_4567890123456789012345;
