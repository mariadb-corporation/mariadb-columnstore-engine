/*
* Report average for each query for two date ranges.
*/
select qr1.qr_qd_key
       ,qr1.qr_time
       ,qr2.qr_time
       ,qr3.qr_time
       ,qr4.qr_time
from queryRun qr1
     ,queryRun qr2
     ,queryRun qr3
     ,queryRun qr4
where 	qr1.qr_r_key = 4
	and qr2.qr_r_key = qr1.qr_r_key - 1 and qr2.qr_qd_key = qr1.qr_qd_key
	and qr3.qr_r_key = qr2.qr_r_key - 1 and qr3.qr_qd_key = qr1.qr_qd_key
	and qr4.qr_r_key = qr3.qr_r_key - 1 and qr4.qr_qd_key = qr1.qr_qd_key
group by 1
order by 1;

select qr1.qr_qd_key
       ,qd.qd_name
       ,qr1.qr_time
       ,qr2.qr_time
       ,qr1.qr_time - qr2.qr_time as diff
from queryRun qr1
     ,queryRun qr2
     ,queryDefinition qd
where 	qr1.qr_r_key = 4
	and qr2.qr_r_key = qr1.qr_r_key - 1 and qr2.qr_qd_key = qr1.qr_qd_key 
	and qr1.qr_qd_key = qd.qd_key
group by 1
order by 1;
