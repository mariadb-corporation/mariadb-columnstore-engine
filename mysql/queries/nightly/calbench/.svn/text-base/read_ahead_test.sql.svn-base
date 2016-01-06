-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 read_ahead_test.sql 7 1500
-- --------------------------------------------------------------------------

select 'Read Ahead Test' into @desc_var;		
use tpch1t;



-- select calflushcache();
-- set @stmtdesc_var = '18_pct_case';
-- set @query = 'select count(l_orderkey) from lineitem where l_extendedprice < 960;';
-- \. calbench_start_execute_end_1x.sql				

-- select calflushcache();
-- set @stmtdesc_var = '22_pct_case';
-- set @query = 'select count(l_orderkey) from lineitem where l_extendedprice < 965;';
-- \. calbench_start_execute_end_1x.sql				

select calflushcache();
set @stmtdesc_var = '25_pct_case';
set @query = 'select count(l_orderkey) from lineitem where l_extendedprice < 970;';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
set @stmtdesc_var = '33_pct_case';
set @query = 'select count(l_orderkey) from lineitem where l_extendedprice < 980;';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
set @stmtdesc_var = '42_pct_case';
set @query = 'select count(l_orderkey) from lineitem where l_extendedprice < 990;';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
set @stmtdesc_var = '52_pct_case';
set @query = 'select count(l_orderkey) from lineitem where l_extendedprice < 1000;';
\. calbench_start_execute_end_1x.sql				

-- select calflushcache();
-- set @stmtdesc_var = '62_pct_case';
-- set @query = 'select count(l_orderkey) from lineitem where l_extendedprice < 1010;';
-- \. calbench_start_execute_end_1x.sql				

select calflushcache();
set @stmtdesc_var = 'Scan_1_column';
set @query = 'select count(l_extendedprice) from lineitem where l_extendedprice = 920;';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
set @stmtdesc_var = 'Scan_1_and_read_69';
set @query = 'select count(l_orderkey) from lineitem where l_extendedprice = 920;';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
set @stmtdesc_var = 'Scan_1_and_read_69_x2';
set @query = 'select count(l_orderkey), count(l_linenumber) from lineitem where l_extendedprice = 920;';
\. calbench_start_execute_end_1x.sql				
