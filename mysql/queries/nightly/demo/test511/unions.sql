/* 
Unions at the 4GB TotalUmMemory limit repeated several times to track memory trend.
*/
select count(*) from (select p_partkey from part where p_partkey <= 116000000 union select p_partkey from part where p_partkey <= 116000000) x;
select calgetstats();
select count(*) from (select p_partkey from part where p_partkey <= 116000000 union select p_partkey from part where p_partkey <= 116000000) x;
select calgetstats();
select count(*) from (select p_partkey from part where p_partkey <= 116000000 union select p_partkey from part where p_partkey <= 116000000) x;
select calgetstats();
select count(*) from (select p_partkey from part where p_partkey <= 116000000 union select p_partkey from part where p_partkey <= 116000000) x;
select calgetstats();
select count(*) from (select p_partkey from part where p_partkey <= 116000000 union select p_partkey from part where p_partkey <= 116000000) x;
select calgetstats();
select count(*) from (select p_partkey from part where p_partkey <= 116000000 union select p_partkey from part where p_partkey <= 116000000) x;
select calgetstats();
select count(*) from (select p_partkey from part where p_partkey <= 116000000 union select p_partkey from part where p_partkey <= 116000000) x;
select calgetstats();
select count(*) from (select p_partkey from part where p_partkey <= 116000000 union select p_partkey from part where p_partkey <= 116000000) x;
select calgetstats();
select count(*) from (select p_partkey from part where p_partkey <= 116000000 union select p_partkey from part where p_partkey <= 116000000) x;
select calgetstats();
select count(*) from (select p_partkey from part where p_partkey <= 116000000 union select p_partkey from part where p_partkey <= 116000000) x;
select calgetstats();
