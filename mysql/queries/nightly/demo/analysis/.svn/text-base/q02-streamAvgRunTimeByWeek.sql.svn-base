/*
* Report stream average by week.
*/
select  round(datediff(r_date, '2009-07-25') / 7, 0) + 1 as Week,
        round(sum(qr_time)/count(distinct r_key), 2) as StreamAvgForWeek,
        count(distinct r_key) as runCount,
        min(r_date),
        max(r_date)
from    run,
        queryRun,
        queryDefinition
where   r_key = qr_r_key and
        r_good and
        qr_qd_key = qd_key
--      and
--      not qd_physicalIO
group by 1
order by 1;

