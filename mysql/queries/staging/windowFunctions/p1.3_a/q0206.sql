SELECT CUME_DIST(salary) OVER (PARTITION BY depname ORDER BY  enroll_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), salary, empno, enroll_date, depname from empsalary where empno < 10000;
