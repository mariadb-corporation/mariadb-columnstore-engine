drop table if exists myisam_lineitem;
drop table if exists idb_lineitem;

create table idb_lineitem (
        l_orderkey int,
        l_partkey int,
        l_suppkey int,
        l_linenumber bigint,
        l_quantity decimal(12,2),
        l_extendedprice decimal(12,2),
        l_discount decimal(12,2),
        l_tax decimal(12,2),
        l_returnflag char (1),
        l_linestatus char (1),
        l_shipdate date,
        l_commitdate date,
        l_receiptdate date,
        l_shipinstruct char (25),
        l_shipmode char (10),
        l_comment varchar (44)
) engine=infinidb;

set autocommit = 0;
insert into idb_lineitem select * from lineitem limit 1000;
select count(*) from idb_lineitem; -- should be 1000

insert idb_lineitem (l_orderkey, l_partkey) select l_orderkey, l_partkey from lineitem, orders where 
l_orderkey=o_orderkey and l_orderkey<1000;
select count(*) from idb_lineitem; -- should be 2004

insert into idb_lineitem (l_shipdate, l_partkey) (select  now(), l_partkey from lineitem where l_orderkey <= 1000 order by l_orderkey limit 100);
select count(*) from idb_lineitem; -- should be 2104
rollback;

create table myisam_lineitem (
        l_orderkey int,
        l_partkey int,
        l_suppkey int,
        l_linenumber bigint,
        l_quantity decimal(12,2),
        l_extendedprice decimal(12,2),
        l_discount decimal(12,2),
        l_tax decimal(12,2),
        l_returnflag char (1),
        l_linestatus char (1),
        l_shipdate date,
        l_commitdate date,
        l_receiptdate date,
        l_shipinstruct char (25),
        l_shipmode char (10),
        l_comment varchar (44)
);
insert into myisam_lineitem select * from lineitem where l_orderkey <= 500 and l_linenumber = 1; 
select count(*) from myisam_lineitem; -- should be 127
insert into myisam_lineitem (l_orderkey) select count(*) from lineitem group by l_linenumber order by l_linenumber;
select count(*) from myisam_lineitem; -- should be 134

insert idb_lineitem (l_orderkey, l_shipdate) (select o_orderkey, o_orderdate from orders where o_orderkey <= 400);
select count(*) from idb_lineitem; -- should be 103

-- should get mixed engine type error.
insert into idb_lineitem (l_orderkey) select o_orderkey from orders, myisam_lineitem where l_orderkey = o_orderkey;
select count(*) from idb_lineitem; -- should be 233
rollback;

insert low_priority idb_lineitem (l_orderkey) select l_orderkey from lineitem limit 1000;
insert ignore idb_lineitem (l_orderkey) select l_orderkey from lineitem  where l_orderkey < 100 order by 1 ON DUPLICATE KEY UPDATE l_orderkey = 5 ;
select count(*) from idb_lineitem; -- should be 1105
rollback;

