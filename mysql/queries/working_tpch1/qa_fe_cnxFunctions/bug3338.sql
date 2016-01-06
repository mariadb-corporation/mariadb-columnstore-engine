select case substr(dtm,12,2) when '06' then 'L' when '08' then 'M' end daypart, count(*)
  from dtypes group by 1 order by 1;
select case substr(dtm,12,2) when '06' then 'L' when '08' then 'M' else 'Z' end daypart, count(*)
  from dtypes group by 1 order by 1;
select substr(dtm,12,2), case substr(dtm,12,2) when '06' then 'L' when '08' then 'M' end daypart, count(*)
  from dtypes group by 1,2 order by 1;
select substr(dtm,12,2), case substr(dtm,12,2) when '06' then 'L' when '08' then 'M' else 'Z' end daypart, count(*)
  from dtypes group by 1,2 order by 1;
