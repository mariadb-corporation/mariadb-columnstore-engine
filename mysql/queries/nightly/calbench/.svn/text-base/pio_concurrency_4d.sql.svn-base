-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 pio_concurrency_4d.sql 16 5000
-- --------------------------------------------------------------------------

select 'PIO Concurrency 4' into @desc_var;		
use tpch1t;

\! \. reset_concurrent_var.sh 4

select calflushcache();
select sleep(2);

-- set @stmtdesc_var = 'no-op';
-- set @query = 'select ''x'' from dual;';
-- \. calbench_start_execute_end_1x.sql				
				

set @stmtdesc_var = 'discount';
set @query = 'select count(l_discount) from lineitem where l_discount = .05;';
\. calbench_start_execute_end_1x_no_out.sql				

set @stmtdesc_var = 'linenumber';
set @query = 'select count(l_linenumber) from lineitem where l_linenumber = 7;';
\. calbench_start_execute_end_1x_no_out.sql	

set @stmtdesc_var = 'quantity';
set @query = 'select count(l_quantity) from lineitem where l_quantity = 7;';
\. calbench_start_execute_end_1x_no_out.sql				

set @stmtdesc_var = 'extendedprice';
set @query = 'select count(l_extendedprice) from lineitem where l_extendedprice < 1000;';
\. calbench_start_execute_end_1x_no_out.sql