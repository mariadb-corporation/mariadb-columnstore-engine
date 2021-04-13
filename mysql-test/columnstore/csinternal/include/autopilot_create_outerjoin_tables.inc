--disable_warnings
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
--enable_warnings
create table t1 (c int);
create table t2 (c int);
create table t3 (c int);
create table t4 (c int);
#
# Insert 2 rows with r_regionkey < 0
# Insert 2 rows with r_regionkey = NULL
insert into region (select r_regionkey * (-1), r_name, r_comment from region where r_regionkey in (1,2) );
insert into region (select null, r_name, r_comment from region where r_regionkey <0);
#
# Insert 3 rows with n_nationkey < 0
# Insert 3 rows with n_nationkey = NULL
insert into nation (select n_nationkey * (-1), n_name, n_regionkey * (-100), n_comment from nation where n_nationkey in (1,2, 3) );
insert into nation (select null, n_name, null, n_comment from nation where n_nationkey <0);
#
# Insert 20 rows with c_custkey < 0
# Insert 20 rows with c_custkey = NULL
insert into customer (select c_custkey * (-1), c_name, c_address, c_nationkey * (-100), c_phone, c_acctbal, c_mktsegment, c_comment from customer where c_custkey between 1 and 20);
insert into customer (select null, c_name, c_address, null, c_phone, c_acctbal, c_mktsegment, c_comment from customer where c_custkey <0);
#
# Insert 25 rows with p_partkey < 0
# Insert 25 rows with p_partkey = NULL
insert into part (select p_partkey * (-1), p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment from part where p_partkey between 1 and 25);
insert into part (select null, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment from part where p_partkey <0);
#
# Insert 10 rows with s_suppkey < 0
# Insert 10 rows with s_suppkey = NULL
insert into supplier (select s_suppkey * (-1), s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment from supplier where s_suppkey between 1 and 10);
insert into supplier (select null, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment from supplier where s_suppkey <0);
#
# Insert 8 rows with ps_partkey < 0
# Insert 8 rows with ps_partkey = NULL
insert into partsupp (select ps_partkey * (-1000), ps_suppkey * (-1000), ps_availqty, ps_supplycost, ps_comment from partsupp where ps_partkey between 1 and 2);
insert into partsupp (select null, null,  ps_availqty, ps_supplycost, ps_comment from partsupp where ps_partkey <0);
#
# Insert 55 rows with o_orderkey < 0
# Insert 55 rows with o_orderkey = NULL
insert into orders (select o_orderkey * (-1), o_custkey * (-10), o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment from orders where o_orderkey between 1 and 200);
insert into orders (select null, null, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment from orders where o_orderkey <0);
#
# Insert 110 rows with l_orderkey < 0
# Insert 110 rows with l_orderkey = NULL
insert into lineitem (select l_orderkey * (-100), l_partkey * (-100), l_suppkey * (-100), l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment from lineitem where l_orderkey between 1 and 100);
insert into lineitem (select null, null, null, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment from lineitem where l_orderkey <0);
#
insert into  t1 values (1), (2), (3);
insert into  t2 values (1), (5);
insert into  t3 values (1), (6);
insert into  t4 values (1), (2), (5), (6);
#
