# Add / drop col.
alter table test012_fact add column newcol varbinary(800);
alter table test012_fact add column newcol2 varbinary(800);
alter table test012_fact drop column newcol2;

# Update the fact table from the staging table.  Do it in chunks as the PmMaxMemorySmallSide is used for the join limit.
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id < 70000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 70000 and 140000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 140000 and 210000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 210000 and 280000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 280000 and 350000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 350000 and 420000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 420000 and 490000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 490000 and 560000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 560000 and 630000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 630000 and 700000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 700000 and 770000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 770000 and 830000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 830000 and 900000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 900000 and 970000 and  test012_fact.id = test012_staging.id;
update test012_fact, test012_staging set test012_fact.c3 = test012_staging.c1 where test012_staging.id between 970000 and 1000000 and  test012_fact.id = test012_staging.id;

# Populate the 10col table.
insert into test012_10col (select id, c1, c1, c1, c1, c1, null, null, null, null, null from test012_staging where id <= 200);
update test012_10col set c10=c1;

