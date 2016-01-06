set @run_var := 1;
\. c_local.sql
\. calbench_start.sql
EXECUTE stmt;
\. calbench_end.sql
\. calbench_start.sql
EXECUTE stmt;
\. calbench_end.sql
\. calbench_start.sql
EXECUTE stmt;
\. calbench_end.sql
\. calbench_start.sql
EXECUTE stmt;
\. calbench_end.sql
\. calbench_start.sql
EXECUTE stmt;
\. calbench_end.sql
