drop database if exists bulkload_features;
create database bulkload_features;
use bulkload_features;

drop table if exists autoinc;
drop table if exists enclosedby;
drop table if exists saturation;
drop table if exists bug3801;
drop table if exists bug3810;
drop table if exists part;
drop table if exists simple;
drop table if exists bug4231;
drop table if exists selectIntoImport;
drop table if exists bug2828notnull;
drop table if exists misc;
drop table if exists misc2;
drop table if exists bug4171a;
drop table if exists bug4171b;
drop table if exists bug4089enclosedcharbadfile;
drop table if exists bug4916enclosedbdry;
drop table if exists bug4342;
drop table if exists bug4342_m3;
drop table if exists binarysigned;
drop table if exists binaryunsigned;
drop table if exists binarychar;
drop table if exists bug4379;

create table autoinc (
  c1 int,
  c2 int
) engine=columnstore comment='autoincrement=c2';

create table enclosedby (
  c1 varchar(40),
  c2 varchar(40),
  c3 varchar(40),
  c4 varchar(40),
  c5 varchar(40)
) engine=columnstore;

create table saturation (
  c1 tinyint,
  c2 smallint,
  c3 int,
  c4 bigint,
  c5 decimal(4,2)
) engine=columnstore;

#  
#Starting with 2.0, decimal cols are created with syscolumn.datatype=4.  Before that, they their datatype integer types.
#The bug3801 table is to test the backwards compatibility of the old syscolumn definition.
#
create table bug3801 (
  id int,
  c1 decimal(10,3),
  c2 decimal(9,2)
) engine=columnstore;

# 
#Decimal(10,3) was a bigint (datatype=9) and decimal(9,2) was an int (datatype=6). 
#
update calpontsys.syscolumn set datatype=9 where tablename='bug3801' and columnname='c1';
update calpontsys.syscolumn set datatype=6 where tablename='bug3801' and columnname='c2';

#
#Test bug3810 involving read buffer copy overflow bug when working with
#data containing embedded enclosed-by character
#
create table bug3810 (
  i1 int,
  c1 varchar(20),
  c2 varchar(20),
  c3 varchar(20),
  c4 varchar(20),
  c5 varchar(20)
) engine=columnstore;

#
#SSB schema part table.  Used for a normal job file cpimport such as "cpimport -j 299"
#
CREATE TABLE `part` (
  `p_partkey` int(11) DEFAULT NULL,
  `p_name` varchar(22) DEFAULT NULL,
  `p_mfgr` char(6) DEFAULT NULL,
  `p_category` char(7) DEFAULT NULL,
  `p_brand1` char(9) DEFAULT NULL,
  `p_color` varchar(11) DEFAULT NULL,
  `p_type` varchar(25) DEFAULT NULL,
  `p_size` int(11) DEFAULT NULL,
  `p_container` char(10) DEFAULT NULL
) engine=columnstore;

#
#SSB schema part table.  Used for a select | cpimport test."
#
CREATE TABLE `selectIntoImport` (
  `p_partkey` int(11) DEFAULT NULL,
  `p_name` varchar(22) DEFAULT NULL,
  `p_mfgr` char(6) DEFAULT NULL,
  `p_category` char(7) DEFAULT NULL,
  `p_brand1` char(9) DEFAULT NULL,
  `p_color` varchar(11) DEFAULT NULL,
  `p_type` varchar(25) DEFAULT NULL,
  `p_size` int(11) DEFAULT NULL,
  `p_container` char(10) DEFAULT NULL
) engine=columnstore;

#
#Simple table for loaded with simple syntax such as "cpimport schema table file".
#
CREATE TABLE `simple` (
  `c_custkey` int(11) DEFAULT NULL,
  `c_name` varchar(25) DEFAULT NULL,
  `c_address` varchar(25) DEFAULT NULL,
  `c_city` char(10) DEFAULT NULL,
  `c_nation` char(15) DEFAULT NULL,
  `c_region` char(12) DEFAULT NULL,
  `c_phone` char(15) DEFAULT NULL,
  `c_mktsegment` char(10) DEFAULT NULL
) engine=columnstore;

create table bug4231 (
c1 int,
c2 int
) engine=columnstore;

#
#Table used to test Bug 2828: Not Null with default
#
create table bug2828notnull (
  reckey  int            not null,
  int1_t  tinyint        not null,
  int1_s  smallint       not null,
  int1_i  int            not null,
  int1_b  bigint         not null,
  int1_td decimal (2,1)  not null,
  int1_sd decimal (4,2)  not null,
  int1_id decimal (8,4)  not null,
  int1_bd decimal (12,2) not null,
  int1_dt date           not null,
  int1_tm datetime       not null,
  str1_i  char(4)        not null,
  str1_d  varchar(20)    not null,
  int2_t  tinyint        not null default 11,
  int2_s  smallint       not null default 222,
  int2_i  int            not null default 3333,
  int2_b  bigint         not null default 44444,
  int2_td decimal (2,1)  not null default 1.1,
  int2_sd decimal (4,2)  not null default 2.22,
  int2_id decimal (8,4)  not null default 3.3333,
  int2_bd decimal (12,2) not null default 4.44,
  int2_dt date           not null default '1927-01-07',
  int2_tm datetime       not null default '1924-08-17 03:04:05',
  str2_i  char(4)        not null default 'xyz',
  str2_d  varchar(20)    not null default 'abcxyz',
  int3_t  tinyint                 default 11,
  int3_s  smallint                default 222,
  int3_i  int                     default 3333,
  int3_b  bigint                  default 44444,
  int3_td decimal (2,1)           default 1.1,
  int3_sd decimal (4,2)           default 2.22,
  int3_id decimal (8,4)           default 3.3333,
  int3_bd decimal (12,2)          default 4.44,
  int3_dt date                    default '1957-09-18',
  int3_tm datetime                default '1953-05-02 07:08:09',
  str3_i  char(4)                 default 'xyz',
  str3_d  varchar(20)             default 'abcxyz'
) engine=columnstore;

create table misc (
  c1 int,
  c2 int
) engine=columnstore;

create table misc2 (
  c1 int,
  c2 int
) engine=columnstore;

create table bug4171a (
  c1 int
) engine=columnstore;

create table bug4171b (
  c1 int
) engine=columnstore;

create table bug4089enclosedcharbadfile (
  c1 varchar(100),
  c2 bigint
) engine=columnstore;

create table bug4916enclosedbdry (
  stringId bigint(20) DEFAULT NULL,
  stringName varchar(512) DEFAULT NULL
) engine=columnstore;

create table bug4342 (
  c1 int
) engine=columnstore;

create table bug4342_m3 (
  c1 int
) engine=columnstore;

#
#Binary import enhancement (Bug5027): signed numbers
#
create table binarysigned (
  mode int,
  id   int,
  t1   tinyint,
  s2   smallint,
  i3   int,
  b4   bigint,
  t5   decimal(2,1),
  s6   decimal(4,2),
  i7   decimal(7,2),
  b8   decimal(12,2),
  f9   float,
  d10  double
) engine=columnstore;

#
#Binary import enhancement (Bug5027): unsigned numbers
#
create table binaryunsigned (
  mode int,
  id   int,
  t1   tinyint       unsigned,
  s2   smallint      unsigned,
  i3   int           unsigned,
  b4   bigint        unsigned,
  t5   decimal(2,1)  unsigned,
  s6   decimal(4,2)  unsigned,
  i7   decimal(7,2)  unsigned,
  b8   decimal(12,2) unsigned,
  f9   float         unsigned,
  d10  double        unsigned
) engine=columnstore;

#
#Binary import enhancement (Bug5027): non-numeric columns
#
create table binarychar (
  mode int,
  id   int,
  c1   char(5),
  c2   varchar(11),
  c3   date,
  c4   datetime
) engine=columnstore;

#
#Import with -f option.
#
create table bug4379 (
  c1 int
) engine=columnstore;
