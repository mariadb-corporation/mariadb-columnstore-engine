-- Expect an error here similar to mysql.  This should be a specific error number for this condition that we 
-- document.
-- Here is mysql's error:
-- ERROR 1242 (21000): Subquery returns more than 1 row

select * from sub1 where c1 = (select c1 from sub2) order by 1;
