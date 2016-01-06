-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 pio_concurrency_4a.sql 1 5000
-- --------------------------------------------------------------------------

select 'PIO Concurrency 4' into @desc_var;		
use tpch1t;


select calflushcache();
select sleep(2);

set @stmtdesc_var = 'no-op';
set @query = 'select ''x'' from dual;';
\. calbench_start_execute_end_1x.sql				


set @stmtdesc_var = 'linenumber';
set @query = 'select count(l_linenumber) from lineitem where l_linenumber = 7;';
\. calbench_start_execute_end_1x.sql				

