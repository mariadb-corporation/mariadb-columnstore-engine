SELECT LAG(salary, 2,NULL) OVER (partition by depname), salary, empno, depname, enroll_date from empsalary3 where empno < 10000;
SELECT LAG(salary, 2,NULL) OVER (partition by depname order by enroll_date), salary, empno, depname, enroll_date from empsalary3 where empno < 10000;
SELECT LAG(salary, 2, 0) OVER (PARTITION BY depname ORDER BY salary NULLS FIRST), salary, empno, depname from empsalary3 where empno < 10000;
SELECT LAG(salary, 2, 0) OVER (PARTITION BY depname ORDER BY salary NULLS LAST), salary, empno, depname from empsalary3 where empno < 10000;
SELECT LAG(salary, 2, NULL) OVER (PARTITION BY depname ORDER BY salary RANGE UNBOUNDED preceding), salary, empno, depname from empsalary3 where empno < 10000;
SELECT LAG(salary, 2, NULL) OVER (PARTITION BY depname ORDER BY empno ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), salary, empno, depname from empsalary3 where empno < 10000;
SELECT LAG(salary, 2, 100) OVER (PARTITION BY depname ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), salary, empno, depname from empsalary3 where empno < 10000;
SELECT LAG(salary, 2, 100) OVER (PARTITION BY depname ORDER BY  enroll_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), salary, empno, enroll_date, depname from empsalary3 where empno < 10000;
SELECT LAG(salary, 2, 100) OVER (PARTITION BY depname ORDER BY  enroll_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), salary, empno, enroll_date, depname from empsalary3 where empno < 10000;
SELECT LAG(salary, 2, 100) OVER (PARTITION BY depname ORDER BY  enroll_date RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), salary, empno, enroll_date, depname from empsalary3 where empno < 10000;

SELECT LEAD(salary, 2,NULL) OVER (partition by depname), salary, empno, depname, enroll_date from empsalary3 where empno < 10000;
SELECT LEAD(salary, 2,NULL) OVER (partition by depname order by enroll_date), salary, empno, depname, enroll_date from empsalary3 where empno < 10000;
SELECT LEAD(salary, 2, 0) OVER (PARTITION BY depname ORDER BY salary NULLS FIRST), salary, empno, depname from empsalary3 where empno < 10000;
SELECT LEAD(salary, 2, 0) OVER (PARTITION BY depname ORDER BY salary NULLS LAST), salary, empno, depname from empsalary3 where empno < 10000;
SELECT LEAD(salary, 2, NULL) OVER (PARTITION BY depname ORDER BY salary RANGE UNBOUNDED preceding), salary, empno, depname from empsalary3 where empno < 10000;
SELECT LEAD(salary, 2, NULL) OVER (PARTITION BY depname ORDER BY empno ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), salary, empno, depname from empsalary3 where empno < 10000;
SELECT LEAD(salary, 2, 100) OVER (PARTITION BY depname ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), salary, empno, depname from empsalary3 where empno < 10000;
SELECT LEAD(salary, 2, 100) OVER (PARTITION BY depname ORDER BY  enroll_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), salary, empno, enroll_date, depname from empsalary3 where empno < 10000;
SELECT LEAD(salary, 2, 100) OVER (PARTITION BY depname ORDER BY  enroll_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), salary, empno, enroll_date, depname from empsalary3 where empno < 10000;
SELECT LEAD(salary, 2, 100) OVER (PARTITION BY depname ORDER BY  enroll_date RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), salary, empno, enroll_date, depname from empsalary3 where empno < 10000;

SELECT NTILE(5) over (partition by  depname), salary, depname, empno, enroll_date from empsalary3 where empno < 10000;
SELECT NTILE(5) over (partition by  depname order by salary desc), salary, depname, empno, enroll_date from empsalary3 where empno < 10000;
SELECT NTILE(5) OVER (PARTITION BY depname ORDER BY salary NULLS FIRST), salary, empno, depname from empsalary3 where empno < 10000;
SELECT NTILE(5) OVER (PARTITION BY depname ORDER BY salary NULLS LAST), salary, empno, depname from empsalary3 where empno < 10000;
SELECT NTILE(5) OVER (PARTITION BY depname ORDER BY salary RANGE UNBOUNDED preceding), salary, empno, depname from empsalary3 where empno < 10000;
SELECT NTILE(5) OVER (PARTITION BY depname ORDER BY empno ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), salary, empno, depname from empsalary3 where empno < 10000;
SELECT NTILE(5) OVER (PARTITION BY depname ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), salary, empno, depname from empsalary3 where empno < 10000;


