drop table if exists test009;
drop table if exists test009_delete;
drop table if exists test009_query;

create table if not exists test009 (
        batch int,
        loaddt datetime,
        c1 bigint,
	c2 int,
	c3 float,
	c4 double,
	c5 varchar(20)
)engine=infinidb;

create table test009_query (
	id int not null,
	iteration int not null,
	start datetime,
	stop datetime,
	err bool,
	correctResults bool,
	rowsReturned int,
	PRIMARY KEY (id, iteration)
);

create table test009_delete (
	id int not null,
	what varchar(30) not null,
	start datetime,
	stop datetime,
	PRIMARY KEY (id, what)
);
