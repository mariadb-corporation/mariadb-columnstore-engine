-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 aggregate_3.sql 5 400
-- --------------------------------------------------------------------------
select 'Group by 3' into @desc_var;		
use tpch1t;

select calflushcache();

set @query = 'select l_returnflag,l_discount,l_linenumber, count(*) from lineitem where l_shipdate between ''1994-12-01'' and ''1996-01-31''
     group by 1,2,3 order by 1,2,3;';
\. calbench_start_execute_end.sql				



