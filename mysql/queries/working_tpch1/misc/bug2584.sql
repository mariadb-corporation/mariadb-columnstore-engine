-- Queries from bug 2584.
select * from sub1 where c2 is null or c2 = 99;
select * from sub1 where c1 = 5 or c2 is null;
select * from sub1 where c2 is null or c1 = 5;

select * from sub1 where c2 = null or c2 = 99;
select * from sub1 where c2 = null or c2 != 99;
select * from sub1 where c2 != null or c2 = 99;
select * from sub1 where c2 != null or c2 != 99;

select * from sub1 where c2 in (null, 99);
select * from sub1 where c2 in (null, 1, 2);
select * from sub1 where c2 not in (null, 1, 2);

select r_regionkey from region where r_regionkey+1 in (null, 2,3);
select r_regionkey from region where r_regionkey+1 in (2,null,3);
select r_regionkey from region where r_regionkey+1 not in (null, 2, 3);

select r_regionkey from region where r_regionkey+1 between null and 3;
select r_regionkey from region where r_regionkey+1 not between 3 and null;

-- Queries from bug 1920 are below.

-- From Comment 17.
select x1.varchar_4, y1.char_8 from table100_char as x1, table10_char as
y1 where x1.varchar_4 = y1.char_8;

-- From Comment 14.
select `char_2` from `table10_char` where `char_2` not in ( 'y', 'u', 's' , 'u' , 'c' , 'n' , 'we', 'were', 'when', 'up' );
select cvchar1 from datatypetestm where cvchar1 NOT IN ('a');
select cvchar1 from datatypetestm where cvchar1 NOT IN ('a', 'z');

-- From Comment 9.
SELECT `cdate` , count( `cdate` ) FROM datatypetestm WHERE `cdate` != '2002-01-24' GROUP BY 1 order by 1;
SELECT `cdate` , count( `cdatetime` ) FROM `datatypetestm` WHERE`cdate` < '2004-02-10' GROUP BY 1 order by 1;
SELECT `cdate` , count( `cdate` ) FROM `datatypetestm` WHERE `cdate` != '2002-01-24' GROUP BY 1 order by 1;
SELECT `cdate` , count( `cdatetime` ) FROM `datatypetestm` WHERE cdate < '2004-02-10' GROUP BY 1 order by 1;

-- From Comment 5.
select char_8, varchar_8 from `table10_char` where `char_8` >=`varchar_8`;
select char_8, varchar_8 from `table10_char` where `varchar_8` <`char_8`;

-- From Comment 4.
select * from datatypetestm where cchar1 != 'a';

-- From bug description.  These are also included in the working_tpch1_compare
-- From bug description.  These are also included in the working_tpch1_compare
select * from datatypetestm where cchar1 != 'a';
select * from datatypetestm where cchar1 != 'a';
select * from datatypetestm where cchar1 <> 'a';
select * from datatypetestm where cchar1 <> 'a';
select * from datatypetestm where cchar1 NOT IN ('a');
select * from datatypetestm where cchar1 NOT IN ('a');
select * from datatypetestm where NOT (cchar1 = 'a');
select * from datatypetestm where NOT (cchar1 = 'a');
select * from datatypetestm where cchar8 != 'a';
select * from datatypetestm where cchar8 != 'a';
select * from datatypetestm where cchar8 <> 'a';
select * from datatypetestm where cchar8 <> 'a';
select * from datatypetestm where cchar8 NOT IN ('a');
select * from datatypetestm where cchar8 NOT IN ('a');
select * from datatypetestm where NOT (cchar8 = 'a');
select * from datatypetestm where NOT (cchar8 = 'a');
select * from datatypetestm where cvchar1 != 'a';
select * from datatypetestm where cvchar1 != 'a';
select * from datatypetestm where cvchar1 <> 'a';
select * from datatypetestm where cvchar1 <> 'a';
select * from datatypetestm where cvchar1 NOT IN ('a');
select * from datatypetestm where cvchar1 NOT IN ('a');
select * from datatypetestm where NOT (cvchar1 = 'a');
select * from datatypetestm where NOT (cvchar1 = 'a');

