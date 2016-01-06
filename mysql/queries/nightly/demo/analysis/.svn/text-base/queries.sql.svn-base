/*
mysql> desc run;
+--------+------------+------+-----+---------+----------------+
| Field  | Type       | Null | Key | Default | Extra          |
+--------+------------+------+-----+---------+----------------+
| r_key  | int(11)    | NO   | PRI | NULL    | auto_increment |
| r_date | date       | YES  |     | NULL    |                |
| r_good | tinyint(1) | YES  |     | NULL    |                |
+--------+------------+------+-----+---------+----------------+
3 rows in set (0.00 sec)

mysql> desc queryRun;
+-----------+--------------+------+-----+---------+-------+
| Field     | Type         | Null | Key | Default | Extra |
+-----------+--------------+------+-----+---------+-------+
| qr_qd_key | int(11)      | YES  |     | NULL    |       |
| qr_r_key  | int(11)      | YES  |     | NULL    |       |
| qr_time   | decimal(7,2) | YES  |     | NULL    |       |
+-----------+--------------+------+-----+---------+-------+

mysql> desc querydefinition;
+-----------------------+---------------+------+-----+---------+-------+
| Field                 | Type          | Null | Key | Default | Extra |
+-----------------------+---------------+------+-----+---------+-------+
| qd_key                | int(11)       | YES  |     | NULL    |       |
| qd_name               | varchar(20)   | YES  |     | NULL    |       |
| qd_sql                | varchar(1000) | YES  |     | NULL    |       |
| qd_umJoin             | tinyint(1)    | YES  |     | NULL    |       |
| qd_pmJoin             | tinyint(1)    | YES  |     | NULL    |       |
| qd_aggregation        | tinyint(1)    | YES  |     | NULL    |       |
| qd_groupBy            | tinyint(1)    | YES  |     | NULL    |       |
| qd_orderBy            | tinyint(1)    | YES  |     | NULL    |       |
| qd_casualPartitioning | tinyint(1)    | YES  |     | NULL    |       |
| qd_distinct           | tinyint(1)    | YES  |     | NULL    |       |
| qd_physicalIO         | tinyint(1)    | YES  |     | NULL    |       |
+-----------------------+---------------+------+-----+---------+-------+
11 rows in set (0.00 sec)
*/

/* 
* Report total run time for each run.
*/
select r_key, r_date, sum(qr_time)
from run, queryRun
where r_key = qr_r_key
group by 1, 2
order by 1;

/*
* Report stream average by week.
*/
select 	round(datediff(r_date, '2009-07-25') / 7, 0) + 1 as Week,
	round(sum(qr_time)/count(distinct r_key), 2) as StreamAvgForWeek,
	count(distinct r_key) as runCount,
	min(r_date), 
	max(r_date)
from 	run,
	queryRun,
	queryDefinition
where	r_key = qr_r_key and
	r_good and
	qr_qd_key = qd_key 
--	and
--	not qd_physicalIO
group by 1
order by 1;
	

/*
* Report average stream time for three date ranges.
*/
(
select 	'Range 1',
	min(r_date), 
	max(r_date),
	count(distinct r_key),
	round(sum(qr_time)/count(distinct r_key), 2) as TotalStreamAvg
from 	run,
     	queryRun
where 
	r_key = qr_r_key and
	r_good and
	r_date between '2009-08-04' and '2009-09-28' 
)
union
(
select 	'Range 2',
	min(r_date), 
	max(r_date),
	count(distinct r_key),
	round(sum(qr_time)/count(distinct r_key), 2) as TotalStreamAvg
from 	run,
     	queryRun
where 
	r_key = qr_r_key and
	r_good and
      	r_date between '2009-09-29' and '2009-12-18' 
)
union
(
select 	'Range 3',
	min(r_date), 
	max(r_date),
	count(distinct r_key),
	round(sum(qr_time)/count(distinct r_key), 2) as TotalStreamAvg
from 	run,
     	queryRun
where 
	r_key = qr_r_key and
	r_good and
      	r_date > '2009-12-18' 
);

/*
* Report average for each query for two date ranges.
*/
select 	qd_key, 
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

