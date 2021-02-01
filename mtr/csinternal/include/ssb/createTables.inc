# create ssb tables
#
--disable_warnings
drop table if exists customer;
--enable_warnings
create table customer (
        c_custkey int,
        c_name varchar (25),
        c_address varchar (25),
        c_city char (10),
        c_nation char (15),
        c_region char (12),
        c_phone char (15),
        c_mktsegment char (10)
) engine=Columnstore;
SHOW CREATE TABLE customer;
#
--disable_warnings
drop table if exists dateinfo;
--enable_warnings
create table `dateinfo` (
        d_datekey int,
        d_date char (18),
        d_dayofweek char (9),
        d_month char (9),
        d_year int,
        d_yearmonthnum int,
        d_yearmonth char (7),
        d_daynuminweek int,
        d_daynuminmonth int,
        d_daynuminyear int,
        d_monthnuminyear int,
        d_weeknuminyear int,
        d_sellingseason varchar (12),
        d_lastdayinweekfl tinyint,
        d_lastdayinmonthfl tinyint,
        d_holidayfl tinyint,
        d_weekdayfl tinyint
) engine=Columnstore;
SHOW CREATE TABLE dateinfo;
#
--disable_warnings
drop table if exists lineorder;
--enable_warnings
create table lineorder (
        lo_orderkey bigint,
        lo_linenumber int,
        lo_custkey int,
        lo_partkey int,
        lo_suppkey int,
        lo_orderdate int,
        lo_orderpriority char (15),
        lo_shippriority char (1),
        lo_quantity decimal (12,2),
        lo_extendedprice decimal (12,2),
        lo_ordtotalprice decimal (12,2),
        lo_discount decimal (12,2),
        lo_revenue decimal (12,2),
        lo_supplycost decimal (12,2),
        lo_tax decimal (12,2),
        lo_commitdate int,
        lo_shipmode char (10)
) engine=Columnstore;
SHOW CREATE TABLE lineorder;
#
--disable_warnings
drop table if exists part;
--enable_warnings
create table part (
        p_partkey int,
        p_name varchar (22),
        p_mfgr char (6),
        p_category char (7),
        p_brand1 char (9),
        p_color varchar (11),
        p_type varchar (25),
        p_size int,
        p_container char (10)
) engine=Columnstore;
SHOW CREATE TABLE part;
#
--disable_warnings
drop table if exists supplier;
--enable_warnings
create table supplier (
        s_suppkey int,
        s_name char (25),
        s_address varchar (25),
        s_city char (10),
        s_nation char (15),
        s_region char (12),
        s_phone char (15)
) engine=Columnstore;
SHOW CREATE TABLE supplier;
#
