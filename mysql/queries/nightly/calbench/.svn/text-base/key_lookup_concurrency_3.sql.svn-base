-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 key_lookup_concurrency_3.sql 200 5000
-- --------------------------------------------------------------------------

select 'key_lookup concurrency_3' into @desc_var;		
use tpch1t;

set @stmtdesc_var = 'key_part_3col_1t';
set @query = 'select p_partkey, p_size, p_retailprice from part where  p_partkey = round(200000000 * rand(),0);';

PREPARE stmt from @query;
EXECUTE stmt;

\. calbench_start_execute_end.sql	
