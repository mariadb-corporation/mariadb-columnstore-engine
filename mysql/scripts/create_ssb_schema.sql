drop table if exists lineorder;
drop table if exists part;
drop table if exists supplier;
drop table if exists customer;
drop table if exists dateinfo;

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
) engine=infinidb
;

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
) engine=infinidb
;

create table supplier (
        s_suppkey int,
        s_name char (25),
        s_address varchar (25),
        s_city char (10),
        s_nation char (15),
        s_region char (12),
        s_phone char (15)
) engine=infinidb
;

create table customer (
        c_custkey int,
        c_name varchar (25),
        c_address varchar (25),
        c_city char (10),
        c_nation char (15),
        c_region char (12),
        c_phone char (15),
        c_mktsegment char (10)
) engine=infinidb
;

create table `dateinfo` (
        d_datekey int,
        d_date char (18),
        d_dayofweek char (8),
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
) engine=infinidb
;

/*
If creating a reference MyISAM ssb, add the indexes below.

create index i_c_custkey on customer(c_custkey);
create index i_c_nation on customer(c_nation);

create index i_d_datekey on dateinfo(d_datekey);
create index i_d_date on dateinfo(d_date);
create index i_d_dayofweek on dateinfo(d_dayofweek);
create index i_d_month on dateinfo(d_month);
create index i_d_year on dateinfo(d_year);
create index i_d_yearmonth on dateinfo(d_yearmonth);

create index i_s_suppkey on supplier(s_suppkey);
create index i_s_name on supplier(s_name);
create index i_s_city on supplier(s_city);
create index i_s_nation on supplier(s_nation);
create index i_s_region on supplier(s_region);

create index i_p_partkey on part(p_partkey);
create index i_p_size on part(p_size);

create index i_lo_orderkey on lineorder(lo_orderkey);
create index i_lo_linenumber on lineorder(lo_linenumber);
create index i_lo_custkey on lineorder(lo_custkey);
create index i_lo_partkey on lineorder(lo_partkey);
create index i_lo_suppkey on lineorder(lo_suppkey);
create index i_lo_orderdate on lineorder(lo_orderdate);
create index i_lo_commitdate on lineorder(lo_commitdate);
*/
