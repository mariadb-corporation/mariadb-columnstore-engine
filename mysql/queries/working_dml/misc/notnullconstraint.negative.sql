drop table if exists notnulltest1;
drop table if exists notnulltest2;
drop table if exists notnulltest3;
drop table if exists notnulltest4;
drop table if exists notnulltest5;
drop table if exists notnulltest6;

Create table notnulltest1 (
col1 int not null, col2 smallint not null default 12, c3 varchar(25) default null
)engine=Infinidb;

create table notnulltest2 (col_1 date not null default '0000-00-00') engine=InfiniDB;
create table notnulltest2 (col_1 date not null default '2012-05-00') engine=InfiniDB;
create table notnulltest2 (col_1 date not null default '2012-05-14') engine=InfiniDB;

create table notnulltest3 (col_1 varchar(80) not null default 'this is a test') engine=InfiniDB;
create table notnulltest4 (col_1 varchar(80) not null) engine=InfiniDB;

create table notnulltest5 (col_1 bigint) engine=InfiniDB;
alter table notnulltest5 change column col_1 col_1 bigint not null;

create table notnulltest6 (col_1 bigint) engine=InfiniDB;
insert into notnulltest6 values (1),(2),(3);
select * from notnulltest6;
alter table notnulltest6 add column (col_2 bigint);
select * from notnulltest6;
update notnulltest6 set col_2=100;
select * from notnulltest6;
alter table notnulltest6 change column col_2 col_2 bigint not null;
select * from notnulltest6;

insert into notnulltest1 value (null, default(col2), null);
insert into notnulltest1 value (1, default(col2), null);
insert into notnulltest1 value (1, null, null);
select * from notnulltest1;
update notnulltest1 set col1=null;
select * from notnulltest1;
update notnulltest1 set col1='';
select * from notnulltest1;
update notnulltest1 set col2=null;
select * from notnulltest1;
update notnulltest1 set col2='';
select * from notnulltest1;
update notnulltest1 set c3='a strings column';
select * from notnulltest1;
update notnulltest1 set c3=null;
select * from notnulltest1;

alter table notnulltest1 alter col1 set default 20;
alter table notnulltest1 alter col2 drop default;
update notnulltest1 set col1=null;
select * from notnulltest1;
update notnulltest1 set col1='';
select * from notnulltest1;
update notnulltest1 set col2=null;
select * from notnulltest1;
update notnulltest1 set col2='';
select * from notnulltest1;

insert into notnulltest1 values (null, 10, 'a comment'), (20, 10, 'a comment');
insert into notnulltest1 values (default(col1), 10, 'a comment'), (20, 10, 'a comment');

insert into notnulltest2 values ('0000-00-00');
select * from notnulltest2;
alter table notnulltest2 alter col_1 drop default;

drop table notnulltest1;
drop table notnulltest2;
drop table notnulltest3;
drop table notnulltest4;
drop table notnulltest5;
drop table notnulltest6;