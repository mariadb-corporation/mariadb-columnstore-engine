select cdate, cdatetime, timestampdiff(day, cdate, cdatetime) from datatypetestm;
select cdate, cdatetime, timestampdiff(minute, cdate, cdatetime) from datatypetestm;
select cdate, cdatetime, timestampdiff(second, cdatetime, cdate) from datatypetestm;
select cdate, cdatetime, timestampdiff(microsecond, cdate, cdatetime) from datatypetestm;
select cidx, cdate, timestampdiff(quarter, cdate, '2010-03-27') from datatypetestm;
select cidx, cdate, timestampdiff(year, cdatetime, '2010-03-27') from datatypetestm;
select cidx, cdate, timestampdiff(week, '1997-04-01', cdate) from datatypetestm;
select cidx, cdate, timestampdiff(month, '1997-04-01', cdate) from datatypetestm;
select cidx, cdate, timestampdiff(hour, '1997-04-01 11:20:30', cdatetime) from datatypetestm;
select cidx, CDATETIME from datatypetestm order by TIMESTAMPDIFF(FRAC_SECOND,CDATETIME, '2011-05-06 11:09:36'), cidx;

