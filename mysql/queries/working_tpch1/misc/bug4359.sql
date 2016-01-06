drop table if exists bug4359;

CREATE TABLE bug4359 (
  sdate datetime DEFAULT NULL,
  edate datetime DEFAULT NULL
) ENGINE=InfiniDB;

insert into bug4359 values ('2009-01-05 13:30:00','2009-01-05 14:30:00');
insert into bug4359 values ('2009-01-05 13:30:00','2009-01-05 14:00:00');

select timediff(edate,sdate) from bug4359;
select time_to_sec(timediff(edate,sdate)) from bug4359;

drop table bug4359;

