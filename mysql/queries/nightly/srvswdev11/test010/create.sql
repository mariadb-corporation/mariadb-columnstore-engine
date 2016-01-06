drop table if exists test010;

create table test010(
	batch int, 
	dt date, 
	batchRow int
)engine=infinidb;

drop table if exists tbug5237;

create table tbug5237(
	batch tinyint
)engine=infinidb;
