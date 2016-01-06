-- Queries for bug 2634.  Bug where all rows are being returned for a left join b where a.c1 = b.c1 and b.c1 
-- is null was returning all rows from a.  Should return only the rows from a that do not have a matching row 
-- in b (i.e. a minus b).

-- Customers without orders - 99,996 against tpch1.
select count(distinct c_custkey) from customer join orders on c_custkey = o_custkey; 

-- Customers without orders - 50,004 rows against tpch1.
select count(distinct c_custkey) from customer left join orders on c_custkey = o_custkey where o_custkey is null; 
select count(distinct c_custkey) from customer left join orders on c_custkey = o_custkey where isnull(o_custkey);

-- A few misc queries against string types.
select count(*) from table100_char a join table10_char b on a.char_4 = b.char_4;
select count(*) from table100_char a left join table10_char b on a.char_4 = b.char_4;
select count(*) from table100_char a right join table10_char b on a.char_4 = b.char_4;
select count(*) from table100_char a left join table10_char b on a.char_4 = b.char_4 where b.char_4 is null;
select a.char_4 from table100_char a left join table10_char b on a.char_4 = b.char_4 where b.char_4 is null order by 1;

select count(distinct char_9) from table100_char;
select count(distinct a.char_9) from table100_char a join table10_char b on a.char_9 = b.char_9;
select count(distinct a.char_9) from table100_char a left join table10_char b on a.char_9 = b.char_9 where b.char_9 is null;

select count(distinct char_2) from table100_char;
select count(distinct a.char_2) from table100_char a join table10_char b on a.char_2 = b.char_9;
select count(distinct a.char_2) from table100_char a left join table10_char b on a.char_2 = b.char_9 where b.char_9 is null;

select count(distinct b.char_9) from table10_char b;
select count(distinct b.char_9) from table100_char a join table10_char b on a.char_2 = b.char_9;
select count(distinct b.char_9) from table100_char a right join table10_char b on a.char_2 = b.char_9 where a.char_2 is null;

select sub1.c1 s1c1, sub2.c1 s2c1, sub3.c1 s3c1 from sub1 left join sub2 on sub1.c1 = sub2.c1 left join sub3 on sub2.c1 = sub3.c1 where sub3.c1 is null order by 1, 2, 3;

select sub1.c1 s1c1, sub2.c1 s2c1, sub3.c1 s3c1 from sub1 right join sub2 on sub1.c1 = sub2.c1 right join sub3 on sub2.c1 = sub3.c1 where sub1.c1 is null or sub2.c1 < 10 or sub1.c1 >= 2 order by 1, 2, 3;

select sub1.c1 s1c1, sub1.c2 s1c2, sub2.c1 s2c1, sub2.c2 s2c2 from sub1 left join sub2 on sub1.c1 = sub2.c1 and sub1.c2 = sub2.c2 order by 1, 2, 3, 4;

select sub1.c1 s1c1, sub1.c2 s1c2, sub2.c1 s2c1, sub2.c2 s2c2 from sub1 left join sub2 on sub1.c1 = sub2.c1 and sub1.c2 = sub2.c2 where sub2.c1 is null order by 1, 2, 3, 4;

