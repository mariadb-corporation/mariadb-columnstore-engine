select max(batchRow) into @rowsPer from test010;
select batch, count(*), (case count(*) when @rowsPer then 'good' else 'bad' end) q1 from test010 group by 1;
select count(distinct batch), (case count(distinct batch) when 2 then 'good' else 'bad' end) q2 from test010;

select calDisablePartitionsByValue('test010', 'batch', '0', '1');
select min(batch), (case min(batch) when 2 then 'good' else 'bad' end) q3 from test010;
select calEnablePartitionsByValue('test010', 'batch', '0', '1');
select min(batch), (case min(batch) when 1 then 'good' else 'bad' end) q4 from test010;
select batch, count(*), (case count(*) when @rowsPer then 'good' else 'bad' end) q5 from test010 group by 1;

select count(distinct dt), (case count(distinct dt) when 2 then 'good' else 'bad' end) q6 from test010;
select calDisablePartitionsByValue('test010', 'dt', '2010-01-01', '2012-10-10');
select count(distinct dt), (case count(distinct dt) when 1 then 'good' else 'bad' end) q7 from test010;
select calEnablePartitionsByValue('test010', 'dt', '2010-01-01', '2012-10-10');
select count(distinct dt), (case count(distinct dt) when 2 then 'good' else 'bad' end) q8 from test010;

select calDropPartitionsByValue('test010', 'dt', '2012-10-10', '2012-10-10');
select count(distinct dt), (case count(distinct dt) when 1 then 'good' else 'bad' end) q9 from test010;
select min(batch), (case min(batch) when 2 then 'good' else 'bad' end) q10 from test010;

# Disabled; made obsolete by changes for bug 5237
#select count(*), (case count(*) when 4 then 'good' else 'bad' end) q11 from tbug3838;
#select calflushcache();
#select count(*), (case count(*) when 4 then 'good' else 'bad' end) q12 from tbug3838;
