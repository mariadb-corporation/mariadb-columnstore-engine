-- not supported . no join prescribed.  This will result in an N-squared problem that will run in calendar time.
select s3 from sub1 where s2 < (select max(s2) from sub5 where sub1.s1 <> sub5.s2) order by 1;
select s2, count(*) from sub5 where s2 < (select max(s2) from sub1 where sub1.s1 <> sub5.s2) group by 1 order by 1;
select count(*) from sub5 where s2 < (select max(s2) from sub2 where sub2.s1 <> sub5.s2) order by 1;

