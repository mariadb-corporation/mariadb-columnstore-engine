DROP TABLE if exists empsalary;

CREATE TABLE empsalary (
    depname varchar(100),
    empno bigint,
    salary int,
    enroll_date date
)engine=columnstore;

insert into empsalary values ('develop'  , 10, 5200, '2007-08-01');
insert into empsalary values ('sales'    ,  1, 5000, '2006-10-01');
insert into empsalary values ('personnel',  5, 3500, '2007-12-10');
insert into empsalary values ('sales'    ,  4, 4800, '2007-08-08');
insert into empsalary values ('personnel',  2, 3900, '2006-12-23');
insert into empsalary values ('develop'  ,  7, 4200, '2008-01-01');
insert into empsalary values ('develop'  ,  9, 4500, '2008-01-01');
insert into empsalary values ('sales'    ,  3, 4800, '2007-08-01');
insert into empsalary values ('develop'  ,  8, 6000, '2006-10-01');
insert into empsalary values ('develop'  , 11, 5200, '2007-08-15');
insert into empsalary values ('develop'  , 12, null, '2008-08-09');

