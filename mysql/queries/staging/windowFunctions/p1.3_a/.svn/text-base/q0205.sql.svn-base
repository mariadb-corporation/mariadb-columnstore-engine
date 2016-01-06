SELECT CUME_DIST(salary) OVER (PARTITION BY depname ORDER BY empno ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), salary, empno, depname from empsalary where empno < 10000;
