-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 pio_scalability.sql 13 600
-- --------------------------------------------------------------------------


select 'PIO Scalability' into @desc_var;		
use tpch1t;


select calflushcache();
-- set @stmtdesc_var = 'Scan4byte';
set @query = 'select /*4 byte*/ count(l_partkey) from lineitem where l_partkey between 100000000 and 100800000 and l_shipdate <''1992-12-31'';';

\. calbench_start_execute_end_1x.sql				

select calflushcache();
-- set @stmtdesc_var = 'Scan4proj3';
set @query = 'select /*4_proj3*/ count(l_linestatus), count(l_suppkey), count(l_orderkey)  
from lineitem where l_partkey between 100000000 and 100800000 and l_shipdate <''1992-12-31'';';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
-- set @stmtdesc_var = 'Scan4proj5';
set @query = 'select /*4_proj5*/ count(l_linestatus), count(l_suppkey), count(l_orderkey),count(l_quantity), count(l_shipdate) 
from lineitem where l_partkey between 100000000 and 100800000 and l_shipdate <''1992-12-31'';';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
-- set @stmtdesc_var = 'Scan4proj7';
set @query = 'select /*4_proj7*/ count(l_linestatus), count(l_suppkey), count(l_orderkey),count(l_quantity), count(l_shipdate),
	count(l_extendedprice), count(l_commitdate)
from lineitem where l_partkey between 100000000 and 100800000 and l_shipdate <''1992-12-31'';';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
-- set @stmtdesc_var = 'Scan4proj9';
set @query = 'select /*4_proj9*/ count(l_linestatus), count(l_suppkey), count(l_orderkey),count(l_quantity), count(l_shipdate),
	count(l_extendedprice), count(l_commitdate), count(l_receiptdate), count(l_discount)
from lineitem where l_partkey between 100000000 and 100800000 and l_shipdate <''1992-12-31'';';
\. calbench_start_execute_end_1x.sql				


select calflushcache();
-- set @stmtdesc_var = 'Scan8byte';
set @query = 'select /*8 byte*/ count(l_extendedprice) from lineitem where l_extendedprice between 1000 and 1250 and l_shipdate <''1992-12-31'';';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
-- set @stmtdesc_var = 'Scan8proj3';
set @query = 'select /*8_proj3*/ count(l_linestatus), count(l_suppkey), count(l_orderkey)  
from lineitem where l_partkey between 100000000 and 100800000 and l_shipdate <''1992-12-31'';';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
-- set @stmtdesc_var = 'Scan8proj5';
set @query = 'select /*8_proj5*/ count(l_linestatus), count(l_suppkey), count(l_orderkey),count(l_quantity), count(l_shipdate) 
from lineitem where l_partkey between 100000000 and 100800000 and l_shipdate <''1992-12-31'';';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
-- set @stmtdesc_var = 'Scan8proj7';
set @query = 'select /*8_proj7*/ count(l_linestatus), count(l_suppkey), count(l_orderkey),count(l_quantity), count(l_shipdate),
	count(l_extendedprice), count(l_commitdate)
from lineitem where l_partkey between 100000000 and 100800000 and l_shipdate <''1992-12-31'';';
\. calbench_start_execute_end_1x.sql				

select calflushcache();
-- set @stmtdesc_var = 'Scan8proj9';
set @query = 'select /*8_proj9*/ count(l_linestatus), count(l_suppkey), count(l_orderkey),count(l_quantity), count(l_shipdate),
	count(l_extendedprice), count(l_commitdate), count(l_receiptdate), count(l_discount)
from lineitem where l_partkey between 100000000 and 100800000 and l_shipdate <''1992-12-31'';';
\. calbench_start_execute_end_1x.sql


select calflushcache();
-- set @stmtdesc_var = 'ScanDict10';
set @query = 'select /*8_proj_dict10*/ count(l_shipmode) from lineitem where l_extendedprice between 1000 and 1250 and l_shipdate <''1992-12-31'';';
\. calbench_start_execute_end_1x.sql

select calflushcache();
-- set @stmtdesc_var = 'ScanDict25';
set @query = 'select /*8_proj_dict25*/ count(l_shipinstruct) from lineitem where l_extendedprice between 1000 and 1250 and l_shipdate <''1992-12-31'';';
\. calbench_start_execute_end_1x.sql


select calflushcache();
-- set @stmtdesc_var = 'ScanDict44';
set @query = 'select /*8_proj_dict44*/ count(l_comment) from lineitem where l_extendedprice between 1000 and 1250 and l_shipdate <''1992-12-31'';';
\. calbench_start_execute_end_1x.sql

