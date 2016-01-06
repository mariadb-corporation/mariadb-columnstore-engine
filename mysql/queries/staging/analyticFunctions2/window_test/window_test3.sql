SELECT CUME_DIST() OVER (PARTITION BY depname ORDER BY empno), salary, empno, depname from empsalary3 where empno < 10000;
SELECT CUME_DIST() OVER (PARTITION BY depname ORDER BY salary NULLS FIRST), salary, empno, depname from empsalary3 where empno < 10000;
SELECT CUME_DIST() OVER (PARTITION BY depname ORDER BY salary NULLS LAST), salary, empno, depname from empsalary3 where empno < 10000;
SELECT CUME_DIST() OVER (PARTITION BY depname ORDER BY salary RANGE UNBOUNDED preceding), salary, empno, depname from empsalary3 where empno < 10000;
SELECT CUME_DIST() OVER (PARTITION BY depname ORDER BY empno ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), salary, empno, depname from empsalary3 where empno < 10000;
SELECT CUME_DIST() OVER (PARTITION BY depname ORDER BY  enroll_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), salary, empno, enroll_date, depname from empsalary3 where empno < 10000;
SELECT CUME_DIST() OVER (PARTITION BY depname ORDER BY  enroll_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), salary, empno, enroll_date, depname from empsalary3 where empno < 10000;
SELECT CUME_DIST() OVER (PARTITION BY depname ORDER BY  enroll_date RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), salary, empno, enroll_date, depname from empsalary3 where empno < 10000;


