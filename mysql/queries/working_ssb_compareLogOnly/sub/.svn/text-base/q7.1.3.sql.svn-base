/*
7.1.3 Standard FROM Clause Query variation 3
And the nesting could continue:
*/

select * from (
  select count(distinct(cnt)) from (
    select cnt, count(*)
    from (select d_month, count(*) cnt
          from dateinfo where d_year = 1992
          group by 1 order by 1) sq
    group by 1 order by 1 desc) sq2
  ) sq3 ;

