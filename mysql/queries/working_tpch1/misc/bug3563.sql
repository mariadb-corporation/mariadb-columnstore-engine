drop table if exists qatabledecimal2byte;
drop table if exists qatabledecimal4byte;
drop table if exists qatabletinyint;
create table qatabledecimal2byte (col1 decimal(1), col2 decimal(4), col3 decimal(4,2)) ENGINE=INFINIDB;
create table qatabledecimal4byte (col1 decimal(5), col2 decimal(9), col3 decimal(9,2)) ENGINE=INFINIDB;
create table qatabletinyint (col numeric(2,0)) ENGINE=INFINIDB;

insert into qatabledecimal2byte values(NULL,NULL,NULL);
insert into qatabledecimal2byte values(null,null,null);
insert into qatabledecimal2byte values(-9, -9999, -99.99);
insert into qatabledecimal2byte values(0, 0, 0);
insert into qatabledecimal2byte values(9, 9999, 99.99);

insert into qatabledecimal4byte values(NULL,NULL,NULL);
insert into qatabledecimal4byte values(null,null,null);
insert into qatabledecimal4byte values(-99999, -999999999, -9999999.99);
insert into qatabledecimal4byte values(0, 0, 0);
insert into qatabledecimal4byte values(99999, 999999999, 9999999.99);

insert into qatabletinyint values (NULL);
insert into qatabletinyint values (null);
insert into qatabletinyint values (-99);
insert into qatabletinyint values (-78);
insert into qatabletinyint values (0);
insert into qatabletinyint values (87);
insert into qatabletinyint values (99);

set infinidb_vtable_mode = 1;
select * from qatabledecimal2byte;
select * from qatabledecimal4byte;
select * from qatabletinyint;
drop table if exists qatabledecimal2byte;
drop table if exists qatabledecimal4byte;
drop table if exists qatabletinyint;




