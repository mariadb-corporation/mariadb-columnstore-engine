/*
* Report average for each query for two date ranges.
*/
select  qd_key,
        qd_name,
        qd_physicalIO as phyIO,
        round(avg(qr1.qr_time),2) as G1Avg,
        round(avg(qr2.qr_time),2) as G2Avg,
        round(avg(qr2.qr_time) - avg(qr1.qr_time), 2) as diff,
        round((avg(qr2.qr_time) - avg(qr1.qr_time)) / avg(qr1.qr_time) * 100, 2) as pctdiff,
        qd_sql
from queryDefinition, queryRun qr1, queryRun qr2, run r1, run r2
where r1.r_key = qr1.qr_r_key and
      r2.r_key = qr2.qr_r_key and
      qr1.qr_qd_key = qd_key and
      qr2.qr_qd_key = qd_key and
      r1.r_date between '2009-08-04' and '2009-09-28' and
--      r2.r_date between '2009-09-29' and '2009-12-18' and
--      r1.r_date between '2009-09-29' and '2009-12-18' and
      r2.r_date > '2009-12-18'

group by 1, 2, 3
-- having pctdiff > 5
order by 6 desc;


