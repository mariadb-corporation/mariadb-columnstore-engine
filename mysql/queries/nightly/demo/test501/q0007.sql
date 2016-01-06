-- From Jim's tpch06_modified.sql script.
-- SELECT SUM(L_EXTENDEDPRICE*L_DISCOUNT) AS REVENUE
-- FROM 	LINEITEM
-- WHERE L_SHIPDATE >= date '1997-01-01' AND
--	L_SHIPDATE < date '1997-01-01' + interval '1' year AND
--	L_DISCOUNT BETWEEN 0.03 - 0.01 AND 0.03 + 0.01 AND
--	L_QUANTITY < 25;

select now();
select now();
select calflushcache();

-- q0007.1.d
SELECT avg(L_EXTENDEDPRICE), avg(L_DISCOUNT) 
FROM 	lineitem
WHERE L_SHIPDATE between '1997-01-01' and '1997-12-31' and
	L_DISCOUNT BETWEEN 0.02 AND 0.04 AND
	L_QUANTITY < 25;

select calgetstats();
select now();

-- q0007.1.c
SELECT avg(L_EXTENDEDPRICE), avg(L_DISCOUNT) 
FROM 	lineitem
WHERE L_SHIPDATE between '1997-01-01' and '1997-12-31' and
	L_DISCOUNT BETWEEN 0.02 AND 0.04 AND
	L_QUANTITY < 25;

select calgetstats();
select now();
