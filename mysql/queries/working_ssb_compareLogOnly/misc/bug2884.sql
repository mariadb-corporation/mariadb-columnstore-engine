select count(*) from dateinfo where d_datekey in (select d_datekey from dateinfo);
select count(*) from dateinfo where d_datekey not in (select d_datekey from dateinfo);
select count(*) from dateinfo where d_month in (select d_month from dateinfo);
select count(*) from dateinfo where d_sellingseason in (select d_sellingseason from dateinfo);

