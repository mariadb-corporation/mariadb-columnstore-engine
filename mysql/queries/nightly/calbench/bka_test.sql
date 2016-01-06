-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 bka_test.sql 5 200
-- --------------------------------------------------------------------------

select 'Batch Key Access' into @desc_var;		
use tpch100;
select calflushcache();


set @query = 'select count(*) FROM part, lineitem WHERE l_partkey=p_partkey AND p_retailprice>2050 and l_discount > 0.04;';
\. calbench_start_execute_end.sql				



