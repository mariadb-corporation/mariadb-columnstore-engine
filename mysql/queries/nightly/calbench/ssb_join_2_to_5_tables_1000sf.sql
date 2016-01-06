-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 ssb_join_2_to_5_tables_1000sf.sql 8 8000
-- --------------------------------------------------------------------------
select 'SSB Join 2-5 tables 1000 scale' into @desc_var;		
use ssb_1000;

select calflushcache();

set @query = 'select count(*) from  supplier, lineorder
     where s_suppkey = lo_suppkey and lo_orderdate <= 19921231
     and s_nation = ''BRAZIL'';';
\. calbench_start_execute_end_2x.sql				

select calflushcache();

set @query = 'select count(*) from dateinfo, supplier, lineorder
     where s_suppkey = lo_suppkey and lo_orderdate <= 19921231
     and d_datekey = lo_orderdate
     and s_nation = ''BRAZIL'';';
\. calbench_start_execute_end_2x.sql				

select calflushcache();

set @query = 'select count(*) from dateinfo, part, supplier, lineorder
     where s_suppkey = lo_suppkey and lo_orderdate <= 19921231
     and d_datekey = lo_orderdate
     and p_partkey = lo_partkey
     and s_nation = ''BRAZIL'';';
\. calbench_start_execute_end_2x.sql				

select calflushcache();

set @query = 'select count(*) from dateinfo,customer, part, supplier, lineorder
     where s_suppkey = lo_suppkey and lo_orderdate <= 19921231
     and d_datekey = lo_orderdate
     and p_partkey = lo_partkey
     and c_custkey = lo_custkey
     and s_nation = ''BRAZIL'';';
\. calbench_start_execute_end_2x.sql				





 
