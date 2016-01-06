-- --------------------------------------------------------------------------
-- \. calbench_nohup_n_times.sh 1 scan_concurrency.sql 20 5000
-- --------------------------------------------------------------------------

select 'scan concurrency' into @desc_var;		
use tpch100;

set @stmtdesc_var = 'discount';
set @query = 'select count(l_discount) from lineitem where l_discount = .05;';

PREPARE stmt from @query;
EXECUTE stmt;

\. calbench_start_execute_end.sql	