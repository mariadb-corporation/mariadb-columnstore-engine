select distinct(d_sellingseason) from dateinfo where d_sellingseason like 'F%' order by 1;
select distinct(d_sellingseason) from dateinfo where d_sellingseason like 'F%' or d_sellingseason like 'W%' order by 1;
select distinct(d_sellingseason) from dateinfo where d_sellingseason like '%r' order by 1;
select distinct(d_sellingseason) from dateinfo where d_sellingseason like '%r' or d_sellingseason like '%r' order by 1;
select distinct(d_sellingseason) from dateinfo where d_sellingseason like '%r' or d_sellingseason like '%g' order by 1;





