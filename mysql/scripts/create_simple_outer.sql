drop table if exists j1;
drop table if exists j6;
drop table if exists j11;
drop table if exists j16; 

create table j1 (j1_key int)engine=infinidb;
create table j6 (j6_key int)engine=infinidb;
create table j11 (j11_key int)engine=infinidb;
create table j16 (j16_key int)engine=infinidb;

insert into j1 values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(null);
insert into j6 values (6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(null);
insert into j11 values (11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(null);
insert into j16 values (16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(null);
