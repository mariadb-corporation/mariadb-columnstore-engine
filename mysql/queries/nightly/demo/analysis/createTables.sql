drop table if exists queryDefinition;
drop table if exists run;
drop table if exists queryRun;
drop table if exists modDefinition;
drop table if exists modRun;


create table queryDefinition (
	qd_key int, 
	qd_name varchar(20), 
	qd_sql varchar(1000), 
	qd_umJoin bool, qd_pmJoin bool, 
	qd_aggregation bool, 
	qd_groupBy bool, 
	qd_orderBy bool, 
	qd_casualPartitioning bool, 
	qd_distinct bool,
	qd_physicalIO bool,
	primary key (qd_key));

create table run (
	r_key int not null auto_increment,
	r_date date,
	r_good bool,
	primary key(r_key));

create table queryRun (
	qr_qd_key int,
	qr_r_key int,
	qr_time decimal(7,2),
	primary key (qr_qd_key, qr_r_key));

create index idx_qr_qd_key on queryRun (qr_qd_key);
create index idx_qr_r_key on queryRun (qr_r_key);

create table modDefinition (
        md_key int,
        md_name varchar(20),
        md_sql varchar(1000),
        primary key (md_key));

create table modRun (
        mr_md_key int,
        mr_r_key int,
        mr_time decimal(7,2),
        primary key (mr_md_key, mr_r_key));

create index idx_mr_md_key on modRun (mr_md_key);
create index idx_mr_r_key on modRun (mr_r_key);

insert into queryDefinition values (1, 'q0001.1.d','Aggregate lineitem, one col + count ', 0,0,1,1,0,1,0,1);
insert into queryDefinition values (2, 'q0001.1.c','Aggregate lineitem, one col + count', 0,0,1,1,0,1,0,0);
insert into queryDefinition values (3, 'q0001.2.d','Aggregate lineitem, three cols + count', 0,0,1,1,0,1,0,1);
insert into queryDefinition values (4, 'q0001.2.c','Aggregate lineitem, three cols + count', 0,0,1,1,0,1,0,0);
insert into queryDefinition values (5, 'q0001.3.d','Aggregate lineitem, five cols + count', 0,0,1,1,0,1,0,1);
insert into queryDefinition values (6, 'q0001.3.c','Aggregate lineitem, five cols + count', 0,0,1,1,0,1,0,0);
insert into queryDefinition values (7, 'q0002.1.d','Join 100K cust vs 100M orders', 0,1,1,1,0,1,0,1);
insert into queryDefinition values (8, 'q0002.1.c','Join 100K cust vs 100M orders', 0,1,1,1,0,1,0,0);
insert into queryDefinition values (9, 'q0002.2.d','Join 100K cust vs 200M orders', 0,1,1,1,0,1,0,1);
insert into queryDefinition values (10, 'q0002.2.c','Join 100K cust vs 200M orders', 0,1,1,1,0,1,0,0);
insert into queryDefinition values (11, 'q0002.3.d','Join 100K cust vs 300M orders', 0,1,1,1,0,1,0,1);
insert into queryDefinition values (12, 'q0002.3.c','Join 100K cust vs 300M orders', 0,1,1,1,0,1,0,0);
insert into queryDefinition values (13, 'q0002.4.d','Join 100K cust vs 400M orders', 0,1,1,1,0,1,0,1);
insert into queryDefinition values (14, 'q0002.4.c','Join 100K cust vs 400M orders', 0,1,1,1,0,1,0,0);
insert into queryDefinition values (15, 'q0002.5.d','Join 100K cust vs 500M orders', 0,1,1,1,0,1,0,1);
insert into queryDefinition values (16, 'q0002.5.c','Join 100K cust vs 500M orders', 0,1,1,1,0,1,0,0);
insert into queryDefinition values (17, 'q0003.1.d','Join 250K orders vs 200M lineitems', 0,1,1,1,0,1,0,1);
insert into queryDefinition values (18, 'q0003.1.c','Join 250K orders vs 200M lineitems', 0,1,1,1,0,1,0,0);
insert into queryDefinition values (19, 'q0003.2.d','Join 250K orders vs 400M lineitems', 0,1,1,1,0,1,0,1);
insert into queryDefinition values (20, 'q0003.2.c','Join 250K orders vs 400M lineitems', 0,1,1,1,0,1,0,0);
insert into queryDefinition values (21, 'q0003.3.d','Join 250K orders vs 600M lineitems', 0,1,1,1,0,1,0,1);
insert into queryDefinition values (22, 'q0003.3.c','Join 250K orders vs 600M lineitems', 0,1,1,1,0,1,0,0);
insert into queryDefinition values (23, 'q0003.4.d','Join 250K orders vs 800M lineitems', 0,1,1,1,0,1,0,1);
insert into queryDefinition values (24, 'q0003.4.c','Join 250K orders vs 800M lineitems', 0,1,1,1,0,1,0,1);
insert into queryDefinition values (25, 'q0004.1.d','Join 1M partsupp vs 200M part', 0,1,1,1,0,0,0,1);
insert into queryDefinition values (26, 'q0004.1.c','Join 1M partsupp vs 200M part', 0,1,1,1,0,0,0,0);
insert into queryDefinition values (27, 'q0004.2.d','Join 1M partsupp vs 400M part', 0,1,1,1,0,0,0,1);
insert into queryDefinition values (28, 'q0004.2.c','Join 1M partsupp vs 400M part', 0,1,1,1,0,0,0,0);
insert into queryDefinition values (29, 'q0004.3.d','Join 1M partsupp vs 600M part', 0,1,1,1,0,0,0,1);
insert into queryDefinition values (30, 'q0004.3.c','Join 1M partsupp vs 600M part', 0,1,1,1,0,0,0,0);
insert into queryDefinition values (31, 'q0004.4.d','Join 1M partsupp vs 800M part', 0,1,1,1,0,0,0,1);
insert into queryDefinition values (32, 'q0004.4.c','Join 1M partsupp vs 800M part', 0,1,1,1,0,0,0,0);
insert into queryDefinition values (33, 'q0005.1.d','Count 400M partsupp', 0,0,1,1,0,0,0,1);
insert into queryDefinition values (34, 'q0005.1.c','Count 400M partsupp', 0,0,1,1,0,0,0,0);
insert into queryDefinition values (35, 'q0005.2.d','Count 800M partsupp', 0,0,1,1,0,0,0,1);
insert into queryDefinition values (36, 'q0005.2.c','Count 800M partsupp', 0,0,1,1,0,0,0,0);
insert into queryDefinition values (37, 'q0005.3.d','Count and aggr 400M partsupp', 0,0,1,1,0,0,0,1);
insert into queryDefinition values (38, 'q0005..3.c','Count and aggr 400M partsupp', 0,0,1,1,0,0,0,0);
insert into queryDefinition values (39, 'q0005.4.d','Count and aggr 800M partsupp', 0,0,1,1,0,0,0,1);
insert into queryDefinition values (40, 'q0005.4.c','Count and aggr 800M partsupp', 0,0,1,1,0,0,0,0);
insert into queryDefinition values (41, 'q0005.5.d','Count 750M 8 byte BigInts: From 1.5 Billion Rows', 0,0,1,0,0,0,0,1);
insert into queryDefinition values (42, 'q0005.5.c','Count 750M 8 byte BigInts: From 1.5 Billion Rows', 0,0,1,0,0,0,0,0);
insert into queryDefinition values (43, 'q0005.6.d','Count 750M 4 byte Ints: From 1.5 Billion Rows', 0,0,1,0,0,0,0,1);
insert into queryDefinition values (44, 'q0005.6.c','Count 750M 4 byte Ints: From 1.5 Billion Rows', 0,0,1,0,0,0,0,0);
insert into queryDefinition values (45, 'q0005.7.d','Aggregate orders.', 0,0,1,1,1,0,0,1);
insert into queryDefinition values (46, 'q0005.7.c','Aggregate orders.', 0,0,1,1,1,0,0,1);
insert into queryDefinition values (47, 'q0005.8.d','Aggregate orders.', 0,0,1,1,1,0,0,1);
insert into queryDefinition values (48, 'q0005.8.c','Aggregate orders.', 0,0,1,1,1,0,0,0);
insert into queryDefinition values (49, 'q0005.9.d','Count 3 Billion Char(1)s: From ~6 Billion Rows', 0,0,1,0,0,0,0,1);
insert into queryDefinition values (50, 'q0005.9.c','Count 3 Billion Char(1)s: From ~6 Billion Rows', 0,0,1,0,0,0,0,0);
insert into queryDefinition values (51, 'q0005.10.d','Count 3 Billion 4 byte Ints: From ~6 Billion Rows', 0,0,1,0,0,0,0,1);
insert into queryDefinition values (52, 'q0005.10.c','Count 3 Billion 4 byte Ints: From ~6 Billion Rows', 0,0,1,0,0,0,0,0);
insert into queryDefinition values (53, 'q0005.11.d','', 0,0,1,1,0,1,0,1);
insert into queryDefinition values (54, 'q0005.11.c','', 0,0,1,1,0,1,0,0);
insert into queryDefinition values (55, 'q0006.1.d','SNPOC:  Scan lineitem and sum 2% of rows.', 0,0,1,0,0,0,0,1);
insert into queryDefinition values (56, 'q0006.2.d','SNPOC:  Scan lineitem and sum 2% of rows.', 0,0,1,0,0,0,0,1);
insert into queryDefinition values (57, 'q0007.1.d','Modified tpch06', 0,0,1,0,0,1,0,1);
insert into queryDefinition values (58, 'q0007.1.c','Modified tpch07', 0,0,1,0,0,1,0,0);
insert into queryDefinition values (59, 'q0008.1','Count', 0,0,1,0,0,0,0,1);
insert into queryDefinition values (60, 'q0008.2','Distinct', 0,0,0,0,0,1,1,0);
insert into queryDefinition values (61, 'q0008.3','Distinct', 0,0,0,0,0,1,1,0);
insert into queryDefinition values (62, 'q0008.4','Distinct', 0,0,0,0,0,1,1,1);
insert into queryDefinition values (63, 'q0008.5','Distinct', 0,0,0,0,0,1,1,0);
insert into queryDefinition values (64, 'q0008.6','Count distinct', 0,0,1,1,0,1,1,1);
insert into queryDefinition values (65, 'q0008.7','Count distinct', 0,0,1,1,0,1,1,1);
insert into queryDefinition values (66, 'q0009.1.d','Join Compares: 1 Million x 800 Million', 0,1,1,1,1,0,0,1);
insert into queryDefinition values (67, 'q0009.1.c','Join Compares: 1 Million x 800 Million', 0,1,1,1,1,0,0,0);
insert into queryDefinition values (68, 'q0009.2.d','Join Compares: 1 Million x 600 Million ', 0,1,1,1,1,0,0,1);
insert into queryDefinition values (69, 'q0009.2.c','Join Compares: 1 Million x 600 Million ', 0,1,1,1,1,0,0,0);
insert into queryDefinition values (70, 'q0009.3.d','Join Compares: 100,000 x 500 Million', 0,1,1,1,1,1,0,1);
insert into queryDefinition values (71, 'q0009.3.c','Join Compares: 100,000 x 500 Million', 0,1,1,1,1,1,0,0);
insert into queryDefinition values (72, 'q0009.4.d','Join Compares: 100,000 x 400 Million', 0,1,1,1,1,1,0,1);
insert into queryDefinition values (73, 'q0009.4.c','Join Compares: 100,000 x 400 Million', 0,1,1,1,1,1,0,0);
insert into queryDefinition values (74, 'q0009.5.d','Join Compares: 100,000 x 300 Million', 0,1,1,1,1,1,0,1);
insert into queryDefinition values (75, 'q0009.5.c','Join Compares: 100,000 x 300 Million', 0,1,1,1,1,1,0,0);
insert into queryDefinition values (76, 'q0009.6.d','Join Compares: 100,000 x 200 Million', 0,1,1,1,1,1,0,1);
insert into queryDefinition values (77, 'q0009.6.c','Join Compares: 100,000 x 200 Million', 0,1,1,1,1,1,0,0);
insert into queryDefinition values (78, 'q0009.7.d','Join Compares: 250,000 x 1 Billion', 0,1,1,1,1,1,0,1);
insert into queryDefinition values (79, 'q0009.7.c','Join Compares: 250,000 x 1 Billion', 0,1,1,1,1,1,0,0);
insert into queryDefinition values (80, 'q0009.8.d','Join Compares: 250,000 x 800 Million', 0,1,1,1,1,1,0,1);
insert into queryDefinition values (81, 'q0009.8.c','Join Compares: 250,000 x 800 Million', 0,1,1,1,1,1,0,0);
insert into queryDefinition values (82, 'q0009.9.d',' Join Compares: 250,000 x 600 Million', 0,1,1,1,1,1,0,1);
insert into queryDefinition values (83, 'q0009.9.c',' Join Compares: 250,000 x 600 Million', 0,1,1,1,1,1,0,0);
insert into queryDefinition values (84, 'q0010.1.d','Join Compares: 3 Million x 800 Million ', 1,0,1,1,1,0,0,0);
insert into queryDefinition values (85, 'q0010.1.c','Join Compares: 3 Million x 800 Million ', 1,0,1,1,1,0,0,0);
insert into queryDefinition values (86, 'q0010.2.d','Join Compares: 1.864 Million x 600 Million', 1,0,1,1,1,0,0,0);
insert into queryDefinition values (87, 'q0010.2.c','Join Compares: 1.864 Million x 600 Million', 1,0,1,1,1,0,0,0);

insert into modDefinition values(1, 'm0001-1.sql', 'create tbl');
insert into modDefinition values(2, 'm0001-2.sql', 'ins 1000');
insert into modDefinition values(3, 'm0001-3.sql', 'drop tbl');
insert into modDefinition values(4, 'm0002-1.sql', 'set autoc');
insert into modDefinition values(5, 'm0002-2.sql', 'upd 3 mil');
insert into modDefinition values(6, 'm0002-3.sql', 'rollback');
insert into modDefinition values(7, 'm0002-4.sql', 'del 3 mil');
insert into modDefinition values(8, 'm0002-5.sql', 'rollback');
insert into modDefinition values(9, 'm0003-1.sql', 'upd 4 mil');
insert into modDefinition values(10, 'm0003-2.sql', 'upd 8 mil');
insert into modDefinition values(11, 'm0003-3.sql', 'upd 12 mil');
insert into modDefinition values(12, 'm0003-4.sql', 'upd 16 mil');
insert into modDefinition values(13, 'm0003-5.sql', 'upd 20 mil');
insert into modDefinition values(14, 'm0003-6.sql', 'upd 24 mil');
insert into modDefinition values(15, 'm0004-1.sql', 'set autoc');
insert into modDefinition values(16, 'm0004-2.sql', 'del 4 mil');
insert into modDefinition values(17, 'm0004-3.sql', 'rollback');
insert into modDefinition values(18, 'm0004-4.sql', 'del 12 mil');
insert into modDefinition values(19, 'm0004-5.sql', 'rollback');
insert into modDefinition values(20, 'm0004-6.sql', 'del 20 mil');
insert into modDefinition values(21, 'm0004-7.sql', 'rollback');

