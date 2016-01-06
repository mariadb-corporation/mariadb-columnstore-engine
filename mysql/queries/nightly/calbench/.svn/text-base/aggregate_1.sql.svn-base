-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 aggregate_1.sql 5 400
-- --------------------------------------------------------------------------
select 'Group by 1' into @desc_var;		
use tpch1t;

select calflushcache();

set @query = 'select l_discount, count(*) from lineitem where l_shipdate between ''1994-12-01'' and ''1996-01-31''
     group by 1 order by 1;';
\. calbench_start_execute_end.sql				



