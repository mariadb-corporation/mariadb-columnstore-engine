/*
7.1.2 Standard FROM Clause Query variation 2
And if we only really wanted to know how many possibilities there were for days in the month (in 1992) we could nest further:
*/

select count(distinct(cnt)) from (
  select cnt, count(*)
  from (select d_month, count(*) cnt
        from dateinfo where d_year = 1992
        group by 1 order by 1) sq
  group by 1 order by 1 desc) sq2 ;

