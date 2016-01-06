/*
Test for partitioning w/ other engine.
*/
drop table if exists part_test;

create table part_test (id int not null, year int not null) partition by
range (year) (partition p0 values less than (2010), partition p2 values less than (2011));

insert into part_test values (1, 2010), (2,2010), (3,2010), (4,2010), (5,2010);
insert into part_test values (6, 2011), (7,2011), (8,2011), (9,2011), (10,2011);
insert into part_test values (6, 2000), (7,2000), (8,2000), (9,2000), (10,2000);
select * from part_test order by 1, 2;
drop table part_test;
