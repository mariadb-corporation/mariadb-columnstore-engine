SELECT DATE_FORMAT(o_orderdate, '%W %M %Y') func_date_format,
	extract(month from o_orderdate) func_extract,
	MINUTE('2008-02-03 10:05:03'),
	SEC_TO_TIME(2378),
	SECOND('10:05:03'),
	TIME_TO_SEC('22:23:00'),
	to_days(o_orderdate),
	week(o_orderdate) func_week,
	year(o_orderdate)
from orders
where o_orderkey=1;

