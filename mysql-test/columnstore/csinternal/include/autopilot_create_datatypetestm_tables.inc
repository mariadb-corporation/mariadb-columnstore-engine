--disable_warnings
drop table if exists datatypetestm;
drop table if exists datatypetestm1;
drop table if exists datatypetestm2;
--enable_warnings
Create table datatypetestm (
CIDX 		INTEGER,
CBIGINT 	BIGINT,
CDECIMAL1 	DECIMAL(1),
CDECIMAL4 	DECIMAL(4),
CDECIMAL4_2 	DECIMAL(4,2),
CDECIMAL5 	DECIMAL(5),
CDECIMAL9 	DECIMAL(9),
CDECIMAL9_2 	DECIMAL(9,2),
CDECIMAL10 	DECIMAL(10),
CDECIMAL18 	DECIMAL(18),
CDECIMAL18_2 	DECIMAL(18,2),
CINTEGER 	INTEGER,
CSMALLINT 	SMALLINT,
CTINYINT 	TINYINT,
CDOUBLE 	DOUBLE,
CFLOAT		FLOAT,
CDATE 		DATE,
CDATETIME 	DATETIME,
CTIME	TIME,
CCHAR1 	CHAR(1),
CCHAR2 	CHAR(2),
CCHAR3 	CHAR(3),
CCHAR4 	CHAR(4),
CCHAR5 	CHAR(5),
CCHAR6 	CHAR(6),
CCHAR7 	CHAR(7),
CCHAR8 	CHAR(8),
CCHAR9 	CHAR(9),
CCHAR255 	CHAR(255),
CVCHAR1 	VARCHAR(1),
CVCHAR2 	VARCHAR(2),
CVCHAR3 	VARCHAR(3),
CVCHAR4 	VARCHAR(4),
CVCHAR5 	VARCHAR(5),
CVCHAR6 	VARCHAR(6),
CVCHAR7 	VARCHAR(7),
CVCHAR8 	VARCHAR(8),
CVCHAR255 	VARCHAR(255),
CTEXT TEXT,
CBLOB BLOB,
CMEDIUMINT MEDIUMINT,
CTINYTEXT TINYTEXT,
CTINYBLOB TINYBLOB,
CMEDIUMTEXT MEDIUMTEXT,
CMEDIUMBLOB MEDIUMBLOB,
CLONGTEXT LONGTEXT,
CLONGBLOB LONGBLOB,
CTIMESTAMP TIMESTAMP
) engine=Columnstore;
#
insert into datatypetestm values (1,-72036854775806,-9,-999,-9.99,-999,-999999,-9999.99,-9999999,-9999999999999,-99999999999.99,-7483646,-766,-26,-1.797693231E+108,-3.402866E+18,'1997-01-01','1997-01-01 00:00:00','13:00:00','a','aa','aaa','aaaa','aaaaa','aaaaaa','aaaaaaa','aaaaaaaa','aaaaaaaaa','aaaaaaaaaa','a','aa','aaa','aaaa','aaaaa','aaaaaa','aaaaaaa','aaaaaaaa','aaaaaaaaaa','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','2020-08-13 03:14:07');
Create table datatypetestm1 (
CIDX 		INTEGER,
CBIGINT 	BIGINT,
CDECIMAL1 	DECIMAL(1),
CDECIMAL4 	DECIMAL(4),
CDECIMAL4_2 	DECIMAL(4,2),
CDECIMAL5 	DECIMAL(5),
CDECIMAL9 	DECIMAL(9),
CDECIMAL9_2 	DECIMAL(9,2),
CDECIMAL10 	DECIMAL(10),
CDECIMAL18 	DECIMAL(18),
CDECIMAL18_2 	DECIMAL(18,2),
CINTEGER 	INTEGER,
CSMALLINT 	SMALLINT,
CTINYINT 	TINYINT,
CDOUBLE 	DOUBLE,
CFLOAT		FLOAT,
CDATE 		DATE,
CDATETIME 	DATETIME,
CCHAR1 	CHAR(1),
CCHAR2 	CHAR(2),
CCHAR3 	CHAR(3),
CCHAR4 	CHAR(4),
CCHAR5 	CHAR(5),
CCHAR6 	CHAR(6),
CCHAR7 	CHAR(7),
CCHAR8 	CHAR(8),
CCHAR9 	CHAR(9),
CCHAR255 	CHAR(255),
CVCHAR1 	VARCHAR(1),
CVCHAR2 	VARCHAR(2),
CVCHAR3 	VARCHAR(3),
CVCHAR4 	VARCHAR(4),
CVCHAR5 	VARCHAR(5),
CVCHAR6 	VARCHAR(6),
CVCHAR7 	VARCHAR(7),
CVCHAR8 	VARCHAR(8),
CVCHAR255 	VARCHAR(255),
CTEXT TEXT,
CBLOB BLOB,
CMEDIUMINT MEDIUMINT,
CTINYTEXT TINYTEXT,
CTINYBLOB TINYBLOB,
CMEDIUMTEXT MEDIUMTEXT,
CMEDIUMBLOB MEDIUMBLOB,
CLONGTEXT LONGTEXT,
CLONGBLOB LONGBLOB,
CTIME TIME,
CTIMESTAMP TIMESTAMP
) engine=Columnstore;
#
insert into datatypetestm1 values (1,-72036854775806,-9,-999,-9.99,-999,-999999,-9999.99,-9999999,-9999999999999,-99999999999.99,-7483646,-766,-26,-1.797693231E+108,-3.402866E+18,'1997-01-01','1997-01-01 00:00:00','a','aa','aaa','aaaa','aaaaa','aaaaaa','aaaaaaa','aaaaaaaa','aaaaaaaaa','aaaaaaaaaa','a','aa','aaa','aaaa','aaaaa','aaaaaa','aaaaaaa','aaaaaaaa','aaaaaaaaaa','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm1 values (2,-72036854775805,-8,-998,-9.98,-998,-999998,-9999.98,-9999998,-9999999999998,-99999999999.98,-7483645,-765,-25,-1.797693230E+108,-3.402865E+18,'1997-01-01','1997-01-01 00:00:01','a','b','c','d','e','f','g','h','i','j','a','b','c','d','e','f','g','h','j','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm1 values (3,-72036854775804,-7,-997,-9.97,-997,-999997,-9999.97,-9999997,-9999999999997,-99999999999.97,-7483644,-764,-24,-1.797693229E+108,-3.402864E+18,'1997-01-02','1997-01-02 00:00:01','a','bb','cc','dd','ee','ff','gg','hh','ii','jj','a','bb','cc','dd','ee','ff','gg','hh','jj','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm1 values (4,-72036854775803,-6,-996,-9.96,-996,-999996,-9999.96,-9999996,-9999999999996,-99999999999.96,-7483643,-763,-23,-1.797693228E+108,-3.402863E+18,'1997-01-03','1997-01-03 00:00:02','a','bb','ccc','ddd','eee','fff','ggg','hhh','iii','jjj','a','bb','ccc','ddd','eee','fff','ggg','hhh','jjj','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm1 values (5,-72036854775802,-5,-995,-9.95,-995,-999995,-9999.95,-9999995,-9999999999995,-99999999999.95,-7483642,-762,-22,-1.797693227E+108,-3.402862E+18,'1997-01-04','1997-01-04 00:00:03','a','bb','ccc','dddd','eeee','ffff','gggg','hhhh','iiii','jjjj','a','bb','ccc','dddd','eeee','ffff','gggg','hhhh','jjjj','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm1 values (6,72036854775803,5,995,9.95,995,999995,9999.95,9999995,9999999999995,99999999999.95,7483643,763,23,1.797693227E+108,3.402862E+18,'2009-12-28','2009-12-31 23:59:56','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','sssssss','rrrrrrr','qqqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','sssssss','qqqqqqq','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm1 values (7,72036854775804,6,996,9.96,996,999996,9999.96,9999996,9999999999996,99999999999.96,7483644,764,24,1.797693228E+108,3.402863E+18,'2009-12-29','2009-12-31 23:59:57','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','rrrrrrrr','qqqqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','qqqqqqqq','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm1 values (8,72036854775805,7,997,9.97,997,999997,9999.97,9999997,9999999999997,99999999999.97,7483645,765,25,1.797693229E+108,3.402864E+18,'2009-12-30','2009-12-31 23:59:58','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','rrrrrrrrr','qqqqqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','qqqqqqqqq','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm1 values (9,72036854775806,8,998,9.98,998,999998,9999.98,9999998,9999999999998,99999999999.98,7483646,766,26,1.797693230E+108,3.402865E+18,'2009-12-31','2009-12-31 23:59:59','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','rrrrrrrrr','qqqqqqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','qqqqqqqqqq','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm1 values (10,72036854775807,9,999,9.99,999,999999,9999.99,9999999,9999999999999,99999999999.99,7483647,767,27,1.797693231E+108,3.402866E+18,'2009-12-31','2009-12-31 23:59:59','z','zz','zzz','zzzz','zzzzz','zzzzzz','zzzzzzz','zzzzzzzz','zzzzzzzzz','zzzzzzzzzz','z','zz','zzz','zzzz','zzzzz','zzzzzz','zzzzzzz','zzzzzzzz','zzzzzzzzzz','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm1 values (11,0,0,0,0.00,0,0,0.00,0,0,0.00,0,0,0,0.00,0.00,'2009-12-31','2009-12-31 23:59:59',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'11:58:28.999999', '2020-08-13 03:14:07');
#
Create table datatypetestm2 (
CIDX 		INTEGER,
CBIGINT 	BIGINT,
CDECIMAL1 	DECIMAL(1),
CDECIMAL4 	DECIMAL(4),
CDECIMAL4_2 	DECIMAL(4,2),
CDECIMAL5 	DECIMAL(5),
CDECIMAL9 	DECIMAL(9),
CDECIMAL9_2 	DECIMAL(9,2),
CDECIMAL10 	DECIMAL(10),
CDECIMAL18 	DECIMAL(18),
CDECIMAL18_2 	DECIMAL(18,2),
CINTEGER 	INTEGER,
CSMALLINT 	SMALLINT,
CTINYINT 	TINYINT,
CDOUBLE 	DOUBLE,
CFLOAT		FLOAT,
CDATE 		DATE,
CDATETIME 	DATETIME,
CCHAR1 	CHAR(1),
CCHAR2 	CHAR(2),
CCHAR3 	CHAR(3),
CCHAR4 	CHAR(4),
CCHAR5 	CHAR(5),
CCHAR6 	CHAR(6),
CCHAR7 	CHAR(7),
CCHAR8 	CHAR(8),
CCHAR9 	CHAR(9),
CCHAR255 	CHAR(255),
CVCHAR1 	VARCHAR(1),
CVCHAR2 	VARCHAR(2),
CVCHAR3 	VARCHAR(3),
CVCHAR4 	VARCHAR(4),
CVCHAR5 	VARCHAR(5),
CVCHAR6 	VARCHAR(6),
CVCHAR7 	VARCHAR(7),
CVCHAR8 	VARCHAR(8),
CVCHAR255 	VARCHAR(255),
CTEXT TEXT,
CBLOB BLOB,
CMEDIUMINT MEDIUMINT,
CTINYTEXT TINYTEXT,
CTINYBLOB TINYBLOB,
CMEDIUMTEXT MEDIUMTEXT,
CMEDIUMBLOB MEDIUMBLOB,
CLONGTEXT LONGTEXT,
CLONGBLOB LONGBLOB,
CTIME TIME,
CTIMESTAMP TIMESTAMP
) engine=Columnstore;
#
insert into datatypetestm2 values (1,-72036854775806,-9,-999,-9.99,-999,-999999,-9999.99,-9999999,-9999999999999,-99999999999.99,-7483646,-766,-26,-1.797693231E+108,-3.402866E+18,'1997-01-01','1997-01-01 00:00:00','a','aa','aaa','aaaa','aaaaa','aaaaaa','aaaaaaa','aaaaaaaa','aaaaaaaaa','aaaaaaaaaa','a','aa','aaa','aaaa','aaaaa','aaaaaa','aaaaaaa','aaaaaaaa','aaaaaaaaaa','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm2 values (2,-72036854775805,-8,-998,-9.98,-998,-999998,-9999.98,-9999998,-9999999999998,-99999999999.98,-7483645,-765,-25,-1.797693230E+108,-3.402865E+18,'1997-01-01','1997-01-01 00:00:01','a','b','c','d','e','f','g','h','i','j','a','b','c','d','e','f','g','h','j','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm2 values (3,-72036854775804,-7,-997,-9.97,-997,-999997,-9999.97,-9999997,-9999999999997,-99999999999.97,-7483644,-764,-24,-1.797693229E+108,-3.402864E+18,'1997-01-02','1997-01-02 00:00:01','a','bb','cc','dd','ee','ff','gg','hh','ii','jj','a','bb','cc','dd','ee','ff','gg','hh','jj','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm2 values (4,-72036854775803,-6,-996,-9.96,-996,-999996,-9999.96,-9999996,-9999999999996,-99999999999.96,-7483643,-763,-23,-1.797693228E+108,-3.402863E+18,'1997-01-03','1997-01-03 00:00:02','a','bb','ccc','ddd','eee','fff','ggg','hhh','iii','jjj','a','bb','ccc','ddd','eee','fff','ggg','hhh','jjj','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm2 values (5,-72036854775802,-5,-995,-9.95,-995,-999995,-9999.95,-9999995,-9999999999995,-99999999999.95,-7483642,-762,-22,-1.797693227E+108,-3.402862E+18,'1997-01-04','1997-01-04 00:00:03','a','bb','ccc','dddd','eeee','ffff','gggg','hhhh','iiii','jjjj','a','bb','ccc','dddd','eeee','ffff','gggg','hhhh','jjjj','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm2 values (6,72036854775803,5,995,9.95,995,999995,9999.95,9999995,9999999999995,99999999999.95,7483643,763,23,1.797693227E+108,3.402862E+18,'2009-12-28','2009-12-31 23:59:56','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','sssssss','rrrrrrr','qqqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','sssssss','qqqqqqq','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm2 values (7,72036854775804,6,996,9.96,996,999996,9999.96,9999996,9999999999996,99999999999.96,7483644,764,24,1.797693228E+108,3.402863E+18,'2009-12-29','2009-12-31 23:59:57','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','rrrrrrrr','qqqqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','qqqqqqqq','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm2 values (8,72036854775805,7,997,9.97,997,999997,9999.97,9999997,9999999999997,99999999999.97,7483645,765,25,1.797693229E+108,3.402864E+18,'2009-12-30','2009-12-31 23:59:58','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','rrrrrrrrr','qqqqqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','qqqqqqqqq','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm2 values (9,72036854775806,8,998,9.98,998,999998,9999.98,9999998,9999999999998,99999999999.98,7483646,766,26,1.797693230E+108,3.402865E+18,'2009-12-31','2009-12-31 23:59:59','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','rrrrrrrrr','qqqqqqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','qqqqqqqqqq','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm2 values (10,72036854775807,9,999,9.99,999,999999,9999.99,9999999,9999999999999,99999999999.99,7483647,767,27,1.797693231E+108,3.402866E+18,'2009-12-31','2009-12-31 23:59:59','z','zz','zzz','zzzz','zzzzz','zzzzzz','zzzzzzz','zzzzzzzz','zzzzzzzzz','zzzzzzzzzz','z','zz','zzz','zzzz','zzzzz','zzzzzz','zzzzzzz','zzzzzzzz','zzzzzzzzzz','texttexttext','blobblobblob',1000,'tinytext','tinyblob','mediumtestmediumtest','mediumblobmediumblob','longtextlongtextlongtext','longbloblongbloblongblob','11:58:28.999999', '2020-08-13 03:14:07');
insert into datatypetestm2 values (11,0,0,0,0.00,0,0,0.00,0,0,0.00,0,0,0,0.00,0.00,'2009-12-31','2009-12-31 23:59:59',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'11:58:28.999999', '2020-08-13 03:14:07');
#

