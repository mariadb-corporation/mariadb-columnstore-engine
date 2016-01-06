-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 scan_repeat.sql 15 1500
-- --------------------------------------------------------------------------


use tpch1t;
select 'Scan Repeat' into @desc_var;		

set @query = 'select /*4 byte*/ count(l_partkey) from lineitem where l_partkey between 100000000 and 100800000 and l_shipdate <''1992-12-31'';';

PREPARE stmt from @query;
EXECUTE stmt;

\. calbench_start_execute_end_1x.sql				
\. calbench_start_execute_end_1x_no_out.sql				
\. calbench_start_execute_end_1x_no_out.sql				
\. calbench_start_execute_end_1x_no_out.sql				
\. calbench_start_execute_end_1x_no_out.sql				

set @query = 'select /*8 byte*/ count(l_extendedprice) from lineitem where l_extendedprice between 1000 and 1250 and l_shipdate <''1992-12-31'';';


PREPARE stmt from @query;
EXECUTE stmt;

\. calbench_start_execute_end_1x.sql				
\. calbench_start_execute_end_1x_no_out.sql				
\. calbench_start_execute_end_1x_no_out.sql				
\. calbench_start_execute_end_1x_no_out.sql				
\. calbench_start_execute_end_1x_no_out.sql				


set @query = 'select /*8 byte_proj5*/ count(l_linestatus), count(l_suppkey), count(l_orderkey), count(l_quantity), count(l_shipdate)  from lineitem where l_extendedprice between 1000 and 1250  and l_shipdate <''1992-12-31'';';


PREPARE stmt from @query;
EXECUTE stmt;

\. calbench_start_execute_end_1x.sql				
\. calbench_start_execute_end_1x_no_out.sql				
\. calbench_start_execute_end_1x_no_out.sql				
\. calbench_start_execute_end_1x_no_out.sql				
\. calbench_start_execute_end_1x_no_out.sql				
