-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 ACB_return_rate_stability_5.sql 10 300
-- --------------------------------------------------------------------------

select 'ACB_rr_repeat_5' into @desc_var;		
use tpch1t;

set @query = 'select  /* RR_10m_5columns */  l_returnflag, l_partkey, l_orderkey, l_quantity, l_shipdate
        from lineitem where l_shipdate < ''1992-01-12'';';
\. calbench_start_execute_end.sql
