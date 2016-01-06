-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 ssb_join_2_to_5_tables_100sf.sql 8 800
-- --------------------------------------------------------------------------
select 'SSB Join 2-5 tables 100GB' into @desc_var;		
use ssb_100;

select calflushcache();

set @query = 'select count(*) from  supplier, lineorder
     where s_suppkey = lo_suppkey
     and s_nation = ''BRAZIL'';';
\. calbench_start_execute_end_2x.sql				

set @query = 'select count(*) from dateinfo, supplier, lineorder
     where s_suppkey = lo_suppkey
     and d_datekey = lo_orderdate
     and s_nation = ''BRAZIL'';';
\. calbench_start_execute_end_2x.sql				

set @query = 'select count(*) from dateinfo, part, supplier, lineorder
     where s_suppkey = lo_suppkey
     and d_datekey = lo_orderdate
     and p_partkey = lo_partkey
     and s_nation = ''BRAZIL'';';
\. calbench_start_execute_end_2x.sql				

set @query = 'select count(*) from dateinfo,customer, part, supplier, lineorder
     where s_suppkey = lo_suppkey
     and d_datekey = lo_orderdate
     and p_partkey = lo_partkey
     and c_custkey = lo_custkey
     and s_nation = ''BRAZIL'';';
\. calbench_start_execute_end_2x.sql				





 
