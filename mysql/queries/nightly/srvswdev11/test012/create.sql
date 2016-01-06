drop table if exists test012_fact;
drop table if exists test012_staging;
drop table if exists test012_10col;
create table if not exists test012_fact (id bigint, c1 varbinary(8), c2 varbinary(200), c3 varbinary(800)) engine=infinidb;
create table if not exists test012_staging (id bigint, c1 varbinary(800)) engine=infinidb;
create table if not exists test012_10col (id bigint, c1 varbinary(800), c2 varbinary(800), c3 varbinary(800), c4 varbinary(800), c5 varbinary(800), c6 varbinary(800),c7 varbinary(800), c8 varbinary(800),c9 varbinary(800), c10 varbinary(800) ) engine=infinidb;
create table if not exists neg_test_012 (id bigint,c1 varbinary(8001)) engine=infinidb;
