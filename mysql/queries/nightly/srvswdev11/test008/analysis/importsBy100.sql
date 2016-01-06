select truncate((id-1)/100,0) grp, min(id), max(id), count(*), timediff(max(stop), min(start)) from test008_import group by 1 order by 1;
