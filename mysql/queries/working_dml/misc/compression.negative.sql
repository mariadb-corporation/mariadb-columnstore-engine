create database if not exists dml;
use dml;

set infinidb_compression_type=0;

drop table if exists comptest;
create table comptest(u1 int, u2 int)engine=infinidb;
select columnname, compressiontype from calpontsys.syscolumn where `schema`='dml' and tablename='comptest' order by 1;

drop table if exists comptest;
create table comptest(c1 int, c2 int)engine=infinidb comment='compression=1';
select columnname, compressiontype from calpontsys.syscolumn where `schema`='dml' and tablename='comptest' order by 1;

drop table if exists comptest;
create table comptest(u1 int, u2 int)engine=infinidb comment='compression=0';
select columnname, compressiontype from calpontsys.syscolumn where `schema`='dml' and tablename='comptest' order by 1;

drop table if exists comptest;
create table comptest(u1 int, c1 int comment 'compression=1')engine=infinidb;
select columnname, compressiontype from calpontsys.syscolumn where `schema`='dml' and tablename='comptest' order by 1;

drop table if exists comptest;
create table comptest(c1 int comment 'compression=1', c2 int comment 'compression=1')engine=infinidb;
select columnname, compressiontype from calpontsys.syscolumn where `schema`='dml' and tablename='comptest' order by 1;

drop table if exists comptest;
create table comptest(u1 int comment 'compression=0', u2 int comment 'compression=0')engine=infinidb;
select columnname, compressiontype from calpontsys.syscolumn where `schema`='dml' and tablename='comptest' order by 1;

set infinidb_compression_type=1;

drop table if exists comptest;
create table comptest(c1 int, c2 int)engine=infinidb;
select columnname, compressiontype from calpontsys.syscolumn where `schema`='dml' and tablename='comptest' order by 1;

drop table if exists comptest;
create table comptest(u1 int, u2 int)engine=infinidb comment='compression=0';
select columnname, compressiontype from calpontsys.syscolumn where `schema`='dml' and tablename='comptest' order by 1;

drop table if exists comptest;
create table comptest(c1 int, c2 int)engine=infinidb comment='compression=1';
select columnname, compressiontype from calpontsys.syscolumn where `schema`='dml' and tablename='comptest' order by 1;

drop table if exists comptest;
create table comptest(c1 int comment 'compression=1', c2 int comment 'compression=1')engine=infinidb;
select columnname, compressiontype from calpontsys.syscolumn where `schema`='dml' and tablename='comptest' order by 1;

drop table if exists comptest;
create table comptest(u1 int comment 'compression=0', u2 int comment 'compression=0')engine=infinidb;
select columnname, compressiontype from calpontsys.syscolumn where `schema`='dml' and tablename='comptest' order by 1;

# Negative test cases.
drop table if exists comptest;
create table comptest(u1 int comment 'compression=3', u2 int comment 'compression=0')engine=infinidb;
create table comptest(u1 int, u2 int)engine=infinidb comment='compression=5';
