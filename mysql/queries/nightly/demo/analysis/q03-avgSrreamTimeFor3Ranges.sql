/*
* Report average stream time for three date ranges.
*/
(
select  'Range 1',
        min(r_date),
        max(r_date),
        count(distinct r_key),
        round(sum(qr_time)/count(distinct r_key), 2) as TotalStreamAvg
from    run,
        queryRun
where
        r_key = qr_r_key and
        r_good and
        r_date between '2009-08-04' and '2009-09-28'
)
union
(
select  'Range 2',
        min(r_date),
        max(r_date),
        count(distinct r_key),
        round(sum(qr_time)/count(distinct r_key), 2) as TotalStreamAvg
from    run,
        queryRun
where
        r_key = qr_r_key and
        r_good and
        r_date between '2009-09-29' and '2009-12-18'
)
union
(
select  'Range 3',
        min(r_date),
        max(r_date),
        count(distinct r_key),
        round(sum(qr_time)/count(distinct r_key), 2) as TotalStreamAvg
from    run,
        queryRun
where
        r_key = qr_r_key and
        r_good and
        r_date > '2009-12-18'
);

