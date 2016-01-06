drop table if exists test008;
drop table if exists test008_import;
drop table if exists test008_query;

create table if not exists test008 (
        batch int,
        loaddt datetime,
        c1 bigint,
	c2 int,
	c3 float,
	c4 double,
	c5 varchar(20)
)engine=infinidb;

create table test008_query (
	id int not null,
	iteration int not null,
	start datetime,
	stop datetime,
	err bool,
	correctResults bool,
	rowsReturned int,
	PRIMARY KEY (id, iteration)
);

create table test008_import (
	id int not null primary key,
	what varchar(30),
	start datetime,
	stop datetime
);
