-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 ACB_tpch_1_6_12_14_3execs 12 3000
-- --------------------------------------------------------------------------

select 'ACB_4_tpch_100' into @desc_var;		
set infinidb_decimal_scale=4;
set infinidb_use_decimal_scale = 'ON';

use test;
select 'TPCH-1' into @stmtdesc_var;
select  sqltext stmt into @query from test.test_queries where id = 17;
use tpch100;
\. calbench_start_execute_end_3x.sql				

select calflushcache();					
use test;
select 'TPCH-6' into @stmtdesc_var;
select  sqltext stmt into @query from test.test_queries where id = 18;
use tpch100;
\. calbench_start_execute_end_3x.sql				

select calflushcache();					
use test;
select 'TPCH-12' into @stmtdesc_var;
select  sqltext stmt into @query from test.test_queries where id = 19;
use tpch100;
\. calbench_start_execute_end_3x.sql				

select calflushcache();					
use test;
select 'TPCH-14' into @stmtdesc_var;
select  sqltext stmt into @query from test.test_queries where id = 20;
use tpch100;
\. calbench_start_execute_end_3x.sql				

