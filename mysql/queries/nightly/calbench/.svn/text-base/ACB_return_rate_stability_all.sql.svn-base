-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 ACB_return_rate_stability_all.sql 5 300
-- --------------------------------------------------------------------------

select 'ACB_rr_repeat_all' into @desc_var;		
use tpch1t;

set @query = 'select  /* RR_lineitem */ *  from lineitem where l_shipdate < ''1992-01-12'';';
\. calbench_start_execute_end.sql

