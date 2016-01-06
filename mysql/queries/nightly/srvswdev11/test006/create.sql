drop table if exists test006;
drop table if exists test006_load;
drop table if exists test006_query;

create table if not exists test006 (
        batch int,
        loaddt datetime,
        c1 bigint,
	c2 int,
	c3 float,
	c4 double,
	c5 varchar(20)
)engine=infinidb;

create table test006_query (
	id int not null,
	iteration int not null,
	start datetime,
	stop datetime,
	err bool,
	correctResults bool,
	rowsReturned int,
	PRIMARY KEY (id, iteration)
);

create table test006_load (
	id int not null primary key,
	what varchar(30),
	start datetime,
	stop datetime
);
