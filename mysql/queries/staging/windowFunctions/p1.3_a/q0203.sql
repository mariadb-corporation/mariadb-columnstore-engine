SELECT CUME_DIST(salary) OVER (PARTITION BY depname ORDER BY empno), salary, empno, depname from empsalary where empno < 10000;
