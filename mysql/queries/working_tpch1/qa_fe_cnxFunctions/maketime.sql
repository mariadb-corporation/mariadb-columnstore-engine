SELECT cidx,CBIGINT,MAKETIME(CBIGINT,CBIGINT,CBIGINT) from datatypetestm;
SELECT cidx,CDECIMAL1,MAKETIME(CDECIMAL1,CDECIMAL1,CDECIMAL1) from datatypetestm;
SELECT cidx,CDECIMAL4,MAKETIME(CDECIMAL4,CDECIMAL4,CDECIMAL4) from datatypetestm;
SELECT cidx,CDECIMAL4_2,MAKETIME(CDECIMAL4_2,CDECIMAL4_2,CDECIMAL4_2) from datatypetestm;
SELECT cidx,CINTEGER,MAKETIME(CINTEGER,CINTEGER,CINTEGER) from datatypetestm;
SELECT cidx,CSMALLINT,MAKETIME(CSMALLINT,CSMALLINT,CSMALLINT) from datatypetestm;
SELECT cidx,CTINYINT,MAKETIME(CTINYINT,CTINYINT,CTINYINT) from datatypetestm;
SELECT cidx,CDOUBLE,MAKETIME(CDOUBLE,CDOUBLE,CDOUBLE) from datatypetestm;
SELECT cidx,CFLOAT,MAKETIME(CFLOAT,CFLOAT,CFLOAT) from datatypetestm;
SELECT cidx,CCHAR2,MAKETIME(CCHAR2,CCHAR2,CCHAR2) from datatypetestm;
SELECT cidx,CDATE,MAKETIME(CDATE,CDATE,CDATE) from datatypetestm;
SELECT cidx,CDATETIME,MAKETIME(CDATETIME,CDATETIME,CDATETIME) from datatypetestm;
SELECT cidx,MAKETIME(CBIGINT,1,10) from datatypetestm;
SELECT cidx,MAKETIME(CBIGINT,-1,10) from datatypetestm;
SELECT cidx,MAKETIME(4,1,10) from datatypetestm;
SELECT cidx,MAKETIME(CCHAR2,'aa',1) from datatypetestm;
SELECT cidx,CDECIMAL1 from datatypetestm where MAKETIME(CBIGINT,CBIGINT,CBIGINT) is null;
SELECT cidx,CDECIMAL4 from datatypetestm where MAKETIME(CDECIMAL4_2,CDECIMAL4_2,CDECIMAL4_2) is not null;
SELECT cidx,CDECIMAL4_2 from datatypetestm where MAKETIME(CTINYINT,CTINYINT,CTINYINT) <> '1';
SELECT cidx,CINTEGER from datatypetestm where MAKETIME(CTINYINT,CTINYINT,CTINYINT) = 1;
SELECT cidx,CSMALLINT from datatypetestm where MAKETIME('1','10','20') = 10;
SELECT cidx,CTINYINT from datatypetestm where MAKETIME(1,4,6) <> "10";

-- bug 3724
drop table if exists b3724;
create table b3724(id int, dtm datetime)engine=infinidb;
insert into b3724 values (1, '2011-06-06 01:01:01'), (2, '2011-06-06 02:02:02'), (3, '2011-06-06 03:03:03'), (4, '2011-06-06 04:04:04');
select id, dtm, time(dtm), maketime(id, id, id) from b3724 where maketime(id, id, id) <= '02:20:00';
drop table b3724;

