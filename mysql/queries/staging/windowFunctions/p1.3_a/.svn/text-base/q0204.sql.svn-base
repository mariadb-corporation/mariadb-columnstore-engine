SELECT CUME_DIST(salary) OVER (PARTITION BY depname ORDER BY salary RANGE UNBOUNDED preceding), salary, empno, depname from empsalary where empno < 10000;
