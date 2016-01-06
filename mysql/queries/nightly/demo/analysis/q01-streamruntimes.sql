/*
* Report total run time for each run.
*/
select r_key, r_date, sum(qr_time)
from run, queryRun
where r_key = qr_r_key
group by 1, 2
order by 1;

