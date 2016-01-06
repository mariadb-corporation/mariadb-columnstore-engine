SELECT CUME_DIST(salary) OVER (PARTITION BY depname ORDER BY  enroll_date RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), salary, empno, enroll_date, depname from empsalary where empno < 10000;
							
