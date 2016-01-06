select 'maketime q1', id,  maketime(id, id, id) from dtypes where maketime(id, id, id) >= '07:40:22' and id between 0 and 59;
select 'maketime q2', id,  maketime(id, id, id) from dtypes where maketime(id, id, id) <= '07:40:22' and id between 0 and 59;
select 'maketime q3', id,  maketime(id, id, id) from dtypes where maketime(id, id, id) <= 074022 and id between 0 and 59;
select 'maketime q4', id,  maketime(id, id, id) from dtypes where maketime(id, id, id) >= 074022 and id between 0 and 59;
select 'maketime q5', id, c1, c2, maketime(id, c1, c2) from dtypes where c1 between 0 and 59 and c2 between 0 and 59 and id between 0 and 59 and maketime(id, c1, c2) >= '39:00:00';
select 'maketime q6', id, c1, c2, maketime(id, c1, c2) from dtypes where c1 between 0 and 59 and c2 between 0 and 59 and id between 0 and 59 and maketime(id, c1, c2) < '39:00:00';

select 'time q1', count(*) from dtypes where time(dtm) < '07:39:22';
select 'time q2', count(*) from dtypes where time(dtm) < 073922;
select 'time q3', count(*) from dtypes where time(dtm) >= '07:39:22';
select 'time q4', count(*) from dtypes where time(dtm) > 073922;
select 'time q5', id, time(dtm) from dtypes;

select 'timediff q1', id, dtm, c1, c2, timediff(time(dtm), maketime(id, c1, c2)) from dtypes where c1 between 0 and 59 and c2 between 0 and 59 and id between 0 and 59;
select 'timediff q2', id, dtm, timediff(time(dtm), time(dtm)) from dtypes;
select 'timediff q3', id, dtm, timediff(dtm, dtm) from dtypes;
select 'timediff q4', id, dtm, c1, c2, timediff(time(dtm), maketime(id, c1, c2)) from dtypes where c1 between 0 and 59 and c2 between 0 and 59 and id between 0 and 59 and timediff(time(dtm), maketime(id, c1, c2)) >= '02:30:00';

select 'cast as time q1', id, dtm, cast(dtm as time) from dtypes where cast(dtm as time) <= '06:46:00';
select 'cast as time q2', id, dtm, cast(dtm as time) from dtypes where cast(dtm as time) <= 064600;

select 'addtime q1', id, dtm, addtime(dtm, '02:22:22') from dtypes where addtime(dtm, '02:22:22') < '2009-01-26';

