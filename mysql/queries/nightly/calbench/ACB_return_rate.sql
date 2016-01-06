-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 ACB_return_rate.sql 10 500
-- --------------------------------------------------------------------------

select 'ACB_rr_base' into @desc_var;		
use tpch1t;

select  count(l_returnflag)  from lineitem where l_shipdate < '1992-02-02';
set @query = 'select  /* RR_10m_1byte_char */  l_returnflag  from lineitem where l_shipdate < ''1992-02-02'';';
\. calbench_start_execute_end_1x.sql				

select  count(l_partkey)  from lineitem where l_shipdate < '1992-02-02';
set @query = 'select  /* RR_10m_4byte_int */  l_partkey  from lineitem where l_shipdate < ''1992-02-02'';';
\. calbench_start_execute_end_1x.sql				

select  count(l_orderkey)  from lineitem where l_shipdate < '1992-02-02';
set @query = 'select  /* RR_10m_8byte_int */  l_orderkey  from lineitem where l_shipdate < ''1992-02-02'';';
\. calbench_start_execute_end_1x.sql				

select  count(l_orderkey)  from lineitem where l_shipdate < '1992-02-02';
set @query = 'select  /* RR_10m_8byte_dec */  l_quantity  from lineitem where l_shipdate < ''1992-02-02'';';
\. calbench_start_execute_end_1x.sql				

select  count(l_shipdate)  from lineitem where l_shipdate < '1992-02-02';
set @query = 'select  /* RR_10m_date */  l_shipdate  from lineitem where l_shipdate < ''1992-02-02'';';
\. calbench_start_execute_end_1x.sql				

select  count(l_shipmode)  from lineitem where l_shipdate < '1992-02-02';
set @query = 'select  /* RR_10m_char_10 */  l_shipmode  from lineitem where l_shipdate < ''1992-02-02'';';
\. calbench_start_execute_end_1x.sql				

select  count(l_comment)  from lineitem where l_shipdate < '1992-02-02';
set @query = 'select  /* RR_10m_char_44 */  l_comment  from lineitem where l_shipdate < ''1992-02-02'';';
\. calbench_start_execute_end_1x.sql				

select  count(l_returnflag), count(l_partkey), count(l_orderkey)  from lineitem where l_shipdate < '1992-02-02';
set @query = 'select  /* RR_10m_3columns */  l_returnflag, l_partkey, l_orderkey   from lineitem where l_shipdate < ''1992-02-02'';';
\. calbench_start_execute_end_1x.sql				

select  count(l_returnflag), count(l_partkey), count(l_orderkey), count(l_quantity), count(l_shipdate)  from lineitem where l_shipdate < '1992-02-02';
set @query = 'select  /* RR_10m_5columns */  l_returnflag, l_partkey, l_orderkey, l_quantity, l_shipdate   
	from lineitem where l_shipdate < ''1992-02-02'';';
\. calbench_start_execute_end_1x.sql				

select  count(l_orderkey),    
count(l_partkey),     
count(l_suppkey),     
count(l_linenumber),  
count(l_quantity),    
count(l_extendedprice),         
count(l_discount),    
count(l_tax),         
count(l_returnflag),  
count(l_linestatus),  
count(l_shipdate),    
count(l_commitdate),  
count(l_receiptdate), 
count(l_shipinstruct),
count(l_shipmode),    
count(l_comment)
  from lineitem where l_shipdate < '1992-02-02';
set @query = 'select  /* RR_lineitem */ *  from lineitem where l_shipdate < ''1992-02-02'';';
\. calbench_start_execute_end_1x.sql				


