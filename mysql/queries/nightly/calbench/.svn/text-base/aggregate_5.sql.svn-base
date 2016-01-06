-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 aggregate_5.sql 5 800
-- --------------------------------------------------------------------------
select 'Group by 5' into @desc_var;		
use tpch1t;

select calflushcache();

set @query = 'select l_returnflag,l_discount,l_linenumber,l_linestatus,l_tax, count(*) from lineitem where l_shipdate between ''1994-12-01'' and ''1996-01-31''
     group by 1,2,3,4,5 order by 1,2,3,4,5;';
\. calbench_start_execute_end.sql				



