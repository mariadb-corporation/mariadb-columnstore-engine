# Negative test run without AllowVarbinary=yes.
drop table if exists test012_fact;
create table if not exists test012_fact (id bigint, c1 varbinary(8), c2 varbinary(200), c3 varbinary(800)) engine=infinidb;
