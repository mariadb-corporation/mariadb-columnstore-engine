-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 2 ACB_return_rate_concurrent.sql 1 300
--                              <2,4,8,16>
-- --------------------------------------------------------------------------

select 'ACB_rr_concurrent' into @desc_var;		
use tpch1t;

set @query = 'select /* RR_1m_1byte */ l_returnflag from lineitem where l_shipdate between ''1992-01-05'' and  ''1992-01-11'';';
\. calbench_start_execute_end.sql				
