/* 
Group by query at the 4GB TotalUmMemory limit repeated several times to track memory trend.
*/
select F, count(*) from (select count(*) F from part where p_partkey <= 75162346 group by p_partkey, p_size) a group by 1;
select calgetstats();
select F, count(*) from (select count(*) F from part where p_partkey <= 75162346 group by p_partkey, p_size) a group by 1;
select calgetstats();
select F, count(*) from (select count(*) F from part where p_partkey <= 75162346 group by p_partkey, p_size) a group by 1;
select calgetstats();
select F, count(*) from (select count(*) F from part where p_partkey <= 75162346 group by p_partkey, p_size) a group by 1;
select calgetstats();
select F, count(*) from (select count(*) F from part where p_partkey <= 75162346 group by p_partkey, p_size) a group by 1;
select calgetstats();
select F, count(*) from (select count(*) F from part where p_partkey <= 75162346 group by p_partkey, p_size) a group by 1;
select calgetstats();
select F, count(*) from (select count(*) F from part where p_partkey <= 75162346 group by p_partkey, p_size) a group by 1;
select calgetstats();
select F, count(*) from (select count(*) F from part where p_partkey <= 75162346 group by p_partkey, p_size) a group by 1;
select calgetstats();
select F, count(*) from (select count(*) F from part where p_partkey <= 75162346 group by p_partkey, p_size) a group by 1;
select calgetstats();
select F, count(*) from (select count(*) F from part where p_partkey <= 75162346 group by p_partkey, p_size) a group by 1;
select calgetstats();
