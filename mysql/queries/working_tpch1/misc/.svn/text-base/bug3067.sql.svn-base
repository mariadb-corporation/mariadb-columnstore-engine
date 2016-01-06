select n_name from ((select n_name, n_regionkey from nation) union all (select n_name, n_regionkey from nation)) as a order by 1;

select n_name from ((select n_name, n_regionkey from nation) union (select n_name, n_regionkey from nation)) as a order by 1;

select a_name from ((select n_name as a_name from nation) union (select n_name as a_name from nation)) as a order by 1;

select name1 from ((select n_name as name1, n_regionkey from nation) union all (select n_name as name1, n_regionkey from nation)) as a order by 1;

select a.a_name, a.regionkey from ((select n_name as a_name, n_regionkey as regionkey from nation) union (select n_name as a_name, n_regionkey as regionkey from nation)) as a order by 1;

select a_name from ((select n_name as a_name from nation) union all (select n_name as a_name from nation)) as a order by 1;

select a.a_name, a.regionkey from ((select n_name as a_name, n_regionkey as regionkey from nation) union all (select n_name as a_name, n_regionkey as regionkey from nation)) as a order by 1;

select * from (select 1 EntityType, n_name EntityName, count(*) Frequency from nation where n_regionkey = 1 group by n_name union all select 2 , n_comment , count(*) Frequency from nation where n_regionkey = 1 group by n_comment) a order by 1, 2;

