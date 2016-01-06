# Rename script from Mark.
# Edits:
# - Changed empty strings to nulls to account for infinidb treating empty strings as nulls.
# - Added autocommit=0;

set autocommit=0;

drop table if exists table1;
drop table if exists table2;
drop table if exists table3;
drop table if exists table4;
drop table if exists qa_col_rename;

create table table1 (col1 int, col2 char(1), col3 char(9), col4 varchar(5), col5 varchar(8)) engine=infinidb;
insert into table1 values (1, 'a', 'aaaaaaaaa', 'abcde', 'bbbbbbbb'), (2, 'b', 'ttttttttt', 'rrrrr', 'zzzzzzzz');
select * from table1;
rename table table1 to table2;
select * from table2;
drop table table2;


create table table1 (col1 int, col2 char(1), col3 char(9), col4 varchar(5), col5 varchar(8)) engine=infinidb;
insert into table1 values (1, 'a', 'aaaaaaaaa', 'abcde', 'bbbbbbbb'), (2, 'b', 'ttttttttt', 'rrrrr', 'zzzzzzzz');
select * from table1;
alter table table1 rename to table2;
select * from table2;
drop table table2;


create table table1 (col1 int, col2 char(1), col3 char(9), col4 varchar(5), col5 varchar(8)) engine=infinidb;
insert into table1 values (1, 'a', 'aaaaaaaaa', 'abcde', 'bbbbbbbb'), (2, 'b', 'ttttttttt', 'rrrrr', 'zzzzzzzz');
select * from table1;
create table table2 (col1 int, col2 char(1), col3 char(9), col4 varchar(5), col5 varchar(8)) engine=infinidb;
insert into table2 values (3, 'c', 'ccccccccc', 'lmnop', 'qqqqqqqq'), (4, 'd', 'ddddddddd', 'ooooo', 'xxxxxxxx');
select * from table2;
rename table table1 to table3, table2 to table4;
select * from table3;
select * from table4;
drop table table3;
drop table table4;


create table table1 (col1 int, col2 char(1), col3 char(9), col4 varchar(5), col5 varchar(8)) engine=infinidb;
insert into table1 values (1, 'a', 'aaaaaaaaa', 'abcde', 'bbbbbbbb'), (2, 'b', 'ttttttttt', 'rrrrr', 'zzzzzzzz');
select * from table1;
create table table2 (col1 int, col2 char(1), col3 char(9), col4 varchar(5), col5 varchar(8)) engine=infinidb;
insert into table2 values (3, 'c', 'ccccccccc', 'lmnop', 'qqqqqqqq'), (4, 'd', 'ddddddddd', 'ooooo', 'xxxxxxxx');
select * from table2;
rename table table1 to tmp_table, table2 to table1, tmp_table to table2;
select * from table1;
select * from table2;
drop table table1;
drop table table2;


Create table qa_col_rename (
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
CVCHAR255 	VARCHAR(255)
) ENGINE=infinidb;

insert into qa_col_rename values (1,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);
insert into qa_col_rename values (2,0,0,0,0,0,0,0,0,0,0,0,0,0,'1400-01-01','1400-01-01 00:00:00',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);
insert into qa_col_rename values (3,0,-9,-9999,-99.99,-99999,-999999999,-9999999.99,-9999999999,-999999999999999999,-9999999999999999.99,-2147483646,-32766,-126,'1400-01-01','1400-01-01 00:00:00','a','aa','aaa','aaaa','aaaaa','aaaaaa','aaaaaaa','aaaaaaaa','aaaaaaaa','aaaaaaaaaa','a','aa','aaa','aaaa','aaaaa','aaaaaa','aaaaaaa','aaaaaaaa','aaaaaaaaaa');
insert into qa_col_rename values (4,-9223372036854775806,-8,-9998,-99.98,-99998,-999999998,-9999999.98,-9999999998,-999999999999999998,-9999999999999999.98,-2147483646,-32766,-126,'1400-01-01','1400-01-01 00:00:01','a','b','c','d','e','f','g','h','i','j','a','b','c','d','e','f','g','h','j');
insert into qa_col_rename values (5,-9223372036854775805,-7,-9997,-99.97,-99998,-999999997,-9999999.97,-9999999997,-999999999999999997,-9999999999999999.97,-2147483645,-32765,-125,'1400-01-02','1400-01-02 00:00:01','a','bb','cc','dd','ee','ff','gg','hh','ii','jj','a','bb','cc','dd','ee','ff','gg','hh','jj');
insert into qa_col_rename values (6,-9223372036854775804,-6,-9996,-99.96,-99998,-999999996,-9999999.96,-9999999996,-999999999999999996,-9999999999999999.96,-2147483644,-32764,-124,'1400-01-03','1400-01-03 00:00:02','a','bb','ccc','ddd','eee','fff','ggg','hhh','iii','jjj','a','bb','ccc','ddd','eee','fff','ggg','hhh','jjj');
insert into qa_col_rename values (7,-9223372036854775803,-5,-9995,-99.95,-99998,-999999995,-9999999.95,-9999999995,-999999999999999995,-9999999999999999.95,-2147483643,-32763,-123,'1400-01-04','1400-01-04 00:00:03','a','bb','ccc','dddd','eeee','ffff','gggg','hhhh','iiii','jjjj','a','bb','ccc','dddd','eeee','ffff','gggg','hhhh','jjjj');
insert into qa_col_rename values (8,-9223372036854775802,-4,-9994,-99.94,-99998,-999999994,-9999999.94,9999999994,-999999999999999994,-9999999999999999.94,-2147483642,-32762,-122,'1400-01-05','1400-01-05 00:00:04','a','bb','ccc','dddd','eeeee','fffff','ggggg','hhhhh','iiiii','jjjjj','a','bb','ccc','dddd','eeeee','fffff','ggggg','hhhhh','jjjjj');
insert into qa_col_rename values (9,9223372036854775803,5,9995,99.95,99995,999999995,9999999.95,9999999995,999999999999999995,9999999999999999.95,2147483643,32763,123,'9999-12-27','9999-12-31 23:59:55','a','bb','ccc','dddd','eeeee','ffffff','gggggg','hhhhhh','iiiiii','jjjjjj','a','bb','ccc','dddd','eeeee','ffffff','gggggg','hhhhhh','jjjjjj');
insert into qa_col_rename values (10,9223372036854775804,6,9996,99.96,99996,999999996,9999999.96,9999999996,999999999999999996,9999999999999999.96,2147483644,32764,124,'9999-12-28','9999-12-31 23:59:56','a','bb','ccc','dddd','eeeee','ffffff','ggggggg','hhhhhhh','iiiiiii','jjjjjjj','a','bb','ccc','dddd','eeeee','ffffff','ggggggg','hhhhhhh','jjjjjjj');
insert into qa_col_rename values (11,9223372036854775805,7,9997,99.97,99997,999999997,9999999.97,9999999997,999999999999999997,9999999999999999.97,2147483645,32765,125,'9999-12-29','9999-12-31 23:59:57','a','bb','ccc','dddd','eeeee','ffffff','ggggggg','hhhhhhhh','iiiiiiii','jjjjjjjj','a','bb','ccc','dddd','eeeee','ffffff','ggggggg','hhhhhhhh','jjjjjjjj');
insert into qa_col_rename values (12,9223372036854775806,8,9998,99.98,99998,999999998,9999999.98,9999999998,999999999999999998,9999999999999999.98,2147483646,32766,126,'9999-12-30','9999-12-31 23:59:58','a','bb','ccc','dddd','eeeee','ffffff','ggggggg','hhhhhhhh','iiiiiiiii','jjjjjjjjj','a','bb','ccc','dddd','eeeee','ffffff','ggggggg','hhhhhhhh','jjjjjjjjj');
insert into qa_col_rename values (13,9223372036854775807,9,9999,99.99,99999,999999999,9999999.99,9999999999,999999999999999999,9999999999999999.99,2147483647,32767,127,'9999-12-31','9999-12-31 23:59:59','a','bb','ccc','dddd','eeeee','ffffff','ggggggg','hhhhhhhh','iiiiiiiii','jjjjjjjjjj','a','bb','ccc','dddd','eeeee','ffffff','ggggggg','hhhhhhhh','jjjjjjjjjj');
insert into qa_col_rename values (14,-9223372036854775806,-9,-9999,-99.99,-99999,-999999999,-9999999.99,-9999999999,-999999999999999999,-9999999999999999.99,-2147483646,-32766,-126,'1400-01-01','1400-01-01 00:00:00','z','y','x','w','v','u','t','s','r','q','z','y','x','w','v','u','t','s','q');
insert into qa_col_rename values (15,-9223372036854775805,-8,-9998,-99.98,-99998,-999999998,-9999999.98,-9999999998,-999999999999999998,-9999999999999999.98,-2147483645,-32765,-125,'1400-01-02','1400-01-02 00:00:01','z','yy','xx','ww','vv','uu','tt','ss','rr','qq','z','yy','xx','ww','vv','uu','tt','ss','qq');
insert into qa_col_rename values (16,-9223372036854775804,-7,-9997,-99.97,-99998,-999999997,-9999999.97,-9999999997,-999999999999999997,-9999999999999999.97,-2147483644,-32764,-124,'1400-01-03','1400-01-03 00:00:02','z','yy','xxx','www','vvvv','uuu','ttt','sss','rrr','qqq','z','yy','xxx','www','vvvv','uuu','ttt','sss','qqq');
insert into qa_col_rename values (17,-9223372036854775803,-6,-9996,-99.96,-99998,-999999996,-9999999.96,-9999999996,-999999999999999996,-9999999999999999.96,-2147483643,-32763,-123,'1400-01-04','1400-01-04 00:00:03','z','yy','xxx','wwww','vvvvv','uuuu','tttt','ssss','rrrr','qqqq','z','yy','xxx','wwww','vvvvv','uuuu','tttt','ssss','qqqq');
insert into qa_col_rename values (18,-9223372036854775802,-5,-9995,-99.95,-99998,-999999995,-9999999.95,-9999999995,-999999999999999995,-9999999999999999.95,-2147483642,-32762,-122,'1400-01-05','1400-01-05 00:00:04','z','yy','xxx','wwww','vvvvv','uuuuu','ttttt','sssss','rrrrr','qqqqq','z','yy','xxx','wwww','vvvvv','uuuuu','ttttt','sssss','qqqqq');
insert into qa_col_rename values (19,9223372036854775803,-4,-9994,-99.94,-99998,-999999994,-9999999.94,9999999994,-999999999999999994,-9999999999999999.94,2147483643,32763,123,'9999-12-27','9999-12-31 23:59:55','z','yy','xxx','wwww','vvvvv','uuuuuu','tttttt','ssssss','rrrrrr','qqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','tttttt','ssssss','qqqqqq');
insert into qa_col_rename values (20,9223372036854775804,5,9995,99.95,99995,999999995,9999999.95,9999999995,999999999999999995,9999999999999999.95,2147483644,32764,124,'9999-12-28','9999-12-31 23:59:56','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','sssssss','rrrrrrr','qqqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','sssssss','qqqqqqq');
insert into qa_col_rename values (21,9223372036854775805,6,9996,99.96,99996,999999996,9999999.96,9999999996,999999999999999996,9999999999999999.96,2147483645,32765,125,'9999-12-29','9999-12-31 23:59:57','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','rrrrrrrr','qqqqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','qqqqqqqq');
insert into qa_col_rename values (22,9223372036854775806,7,9997,99.97,99997,999999997,9999999.97,9999999997,999999999999999997,9999999999999999.97,2147483646,32766,126,'9999-12-30','9999-12-31 23:59:58','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','rrrrrrrrr','qqqqqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','qqqqqqqqq');
insert into qa_col_rename values (23,9223372036854775807,8,9998,99.98,99998,999999998,9999999.98,9999999998,999999999999999998,9999999999999999.98,2147483647,32767,127,'9999-12-31','9999-12-31 23:59:59','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','rrrrrrrrr','qqqqqqqqqq','z','yy','xxx','wwww','vvvvv','uuuuuu','ttttttt','ssssssss','qqqqqqqqqq');
insert into qa_col_rename values (24,0,9,9999,99.99,99999,999999999,9999999.99,9999999999,999999999999999999,9999999999999999.99,2147483647,32767,127,'9999-12-31','9999-12-31 23:59:59','z','z','z','z','z','z','z','z','z','z','z','z','z','z','z','z','z','z','z');
select * from qa_col_rename order by CIDX;

alter table qa_col_rename change CBIGINT 	CBIGINT_R 	BIGINT;
select CBIGINT_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CDECIMAL1 	CDECIMAL1_R 	DECIMAL(1);
select CDECIMAL1_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CDECIMAL4 	CDECIMAL4_R 	DECIMAL(4);
select CDECIMAL4_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CDECIMAL4_2 	CDECIMAL4_2_R 	DECIMAL(4,2);
select CDECIMAL4_2_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CDECIMAL5 	CDECIMAL5_R 	DECIMAL(5);
select CDECIMAL5_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CDECIMAL9 	CDECIMAL9_R 	DECIMAL(9);
select CDECIMAL9_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CDECIMAL9_2 	CDECIMAL9_2_R 	DECIMAL(9,2);
select CDECIMAL9_2_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CDECIMAL10 	CDECIMAL10_R 	DECIMAL(10);
select CDECIMAL10_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CDECIMAL18 	CDECIMAL18_R 	DECIMAL(18);
select CDECIMAL18_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CDECIMAL18_2 	CDECIMAL18_2_R	DECIMAL(18,2);
select CDECIMAL18_2_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CINTEGER 	CINTEGER_R	INTEGER;
select CINTEGER_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CSMALLINT 	CSMALLINT_R	SMALLINT;
select CSMALLINT_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CTINYINT 	CTINYINT_R	TINYINT;
select CTINYINT_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CDATE 		CDATE_R		DATE;
select CDATE_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CDATETIME 	CDATETIME_R	DATETIME;
select CDATETIME_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CCHAR1 	CCHAR1_R	CHAR(1);
select CCHAR1_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CCHAR2 	CCHAR2_R	CHAR(2);
select CCHAR2_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CCHAR3 	CCHAR3_R	CHAR(3);
select CCHAR3_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CCHAR4 	CCHAR4_R	CHAR(4);
select CCHAR4_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CCHAR5 	CCHAR5_R	CHAR(5);
select CCHAR5_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CCHAR6 	CCHAR6_R	CHAR(6);
select CCHAR6_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CCHAR7 	CCHAR7_R	CHAR(7);
select CCHAR7_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CCHAR8 	CCHAR8_R	CHAR(8);
select CCHAR8_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CCHAR9 	CCHAR9_R	CHAR(9);
select CCHAR9_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CCHAR255 	CCHAR255_R	CHAR(255);
select CCHAR255_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CVCHAR1 	CVCHAR1_R	VARCHAR(1);
select CVCHAR1_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CVCHAR2 	CVCHAR2_R	VARCHAR(2);
select CVCHAR2_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CVCHAR3 	CVCHAR3_R	VARCHAR(3);
select CVCHAR3_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CVCHAR4 	CVCHAR4_R	VARCHAR(4);
select CVCHAR4_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CVCHAR5 	CVCHAR5_R	VARCHAR(5);
select CVCHAR5_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CVCHAR6 	CVCHAR6_R	VARCHAR(6);
select CVCHAR6_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CVCHAR7 	CVCHAR7_R	VARCHAR(7);
select CVCHAR7_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CVCHAR8 	CVCHAR8_R	VARCHAR(8);
select CVCHAR8_R from qa_col_rename order by CIDX;

alter table qa_col_rename change CVCHAR255 	CVCHAR255_R	VARCHAR(255);
select CVCHAR255_R from qa_col_rename order by CIDX;

select * from qa_col_rename order by CIDX;

drop table qa_col_rename;
