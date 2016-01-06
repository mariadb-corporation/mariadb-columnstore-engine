-- multiple Windowing function in one statement

SELECT empno, depname, salary, bonus, MIN(bonus) OVER (ORDER BY empno), MAX(bonus) OVER (ORDER BY empno) from (select *, (2013 - extract(YEAR FROM enroll_date)) * 500 as bonus from empsalary3 where (empno >10000) and (empno < 200000) ) s;

-- ORDER by expression
SELECT depname, SUM(salary), RANK() over (order by SUM(salary)) as rank from empsalary3 group by depname;

-- multiple windowing function
SELECT empno, salary, depname, SUM(salary) OVER (PARTITION BY depname ORDER BY salary ASC), rank() OVER (PARTITION BY depname ORDER BY salary ASC), dense_rank() OVER (PARTITION BY depname ORDER BY salary ASC) FROM empsalary3 where empno < 10000;

SELECT empno, depname, enroll_date, salary, AVG(salary) OVER ( ORDER BY depname, enroll_date) from empsalary3 where empno < 10000;

SELECT empno, depname, enroll_date, salary, AVG(salary) OVER ( PARTITION BY (extract(YEAR FROM enroll_date) ) ORDER BY depname) from empsalary3 where empno < 10000;

