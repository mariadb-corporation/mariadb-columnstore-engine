# create dbt3 tables
#
# create nation table
#
--disable_warnings
drop table if exists nation;
--enable_warnings
create table nation (
        n_nationkey int,
        n_name char (25),
        n_regionkey int,
        n_comment varchar (152)
) engine=columnstore;
SHOW CREATE TABLE nation;
#
# create region table
#
--disable_warnings
drop table if exists region;
--enable_warnings
create table region (
        r_regionkey int,
        r_name char (25),
        r_comment varchar (152)
) engine=columnstore;
SHOW CREATE TABLE region;
#
# create customer table
#
--disable_warnings
drop table if exists customer;
--enable_warnings
create table customer (
        c_custkey int,
        c_name varchar (25),
        c_address varchar (40),
        c_nationkey int,
        c_phone char (15),
        c_acctbal decimal(12,2),
        c_mktsegment char (10),
        c_comment varchar (117)
) engine=columnstore;
SHOW CREATE TABLE customer;
#
# create orders table
#
--disable_warnings
drop table if exists orders;
--enable_warnings
create table orders (
        o_orderkey int,
        o_custkey int,
        o_orderstatus char (1),
        o_totalprice decimal(12,2),
        o_orderdate date,
        o_orderpriority char (15),
        o_clerk char (15),
        o_shippriority int,
        o_comment varchar (79)
) engine=columnstore;
SHOW CREATE TABLE orders;
#
# create supplier table
#
--disable_warnings
drop table if exists supplier;
--enable_warnings
create table supplier (
        s_suppkey int,
        s_name char (25),
        s_address varchar (40),
        s_nationkey int,
        s_phone char (15),
        s_acctbal decimal(12,2),
        s_comment varchar (101)
) engine=columnstore;
SHOW CREATE TABLE supplier;
#
# create partsupp table
#
--disable_warnings
drop table if exists partsupp;
--enable_warnings
create table partsupp (
        ps_partkey int,
        ps_suppkey int,
        ps_availqty int,
        ps_supplycost decimal(12,2),
        ps_comment varchar (199)
) engine=columnstore;
SHOW CREATE TABLE partsupp;
#
# create part table
#
--disable_warnings
drop table if exists part;
--enable_warnings
create table part (
        p_partkey int,
        p_name varchar (55),
        p_mfgr char (25),
        p_brand char (10),
        p_type varchar (25),
        p_size int,
        p_container char (10),
        p_retailprice decimal(12,2),
        p_comment varchar (23)
) engine=columnstore;
SHOW CREATE TABLE part;
#
# create lineitem table
#
--disable_warnings
drop table if exists lineitem;
--enable_warnings
create table lineitem (
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
) engine=columnstore;
SHOW CREATE TABLE lineitem;
#
