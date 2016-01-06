-- bug4230
select * from datatypetestm where date(cdatetime) in ('1997-01-01');
select * from datatypetestm where date(cdatetime) in ('1997-01-01', '2009-12-31');
select * from datatypetestm where date(cdatetime) in (cdate, '1997-01-01', '2005-05-05');
select * from datatypetestm where cdate in (date(cdatetime), '1997-01-01', '2009-12-31');

