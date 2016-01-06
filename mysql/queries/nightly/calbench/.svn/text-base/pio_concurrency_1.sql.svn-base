-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 pio_concurrency_1.sql 4 5000
-- --------------------------------------------------------------------------

select 'PIO Concurrency 1' into @desc_var;		
use tpch1t;


select calflushcache();
set @stmtdesc_var = 'linenumber';
set @query = 'select count(l_linenumber) from lineitem where l_linenumber = 7;';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
set @stmtdesc_var = 'quantity';
set @query = 'select count(l_quantity) from lineitem where l_quantity = 7;';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
set @stmtdesc_var = 'extendedprice';
set @query = 'select count(l_extendedprice) from lineitem where l_extendedprice < 1000;';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
set @stmtdesc_var = 'discount';
set @query = 'select count(l_discount) from lineitem where l_discount = .05;';
\. calbench_start_execute_end_1x.sql				
