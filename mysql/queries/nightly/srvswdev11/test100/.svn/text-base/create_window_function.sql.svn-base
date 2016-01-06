--
-- CREATE_TABLE
--
-- CREATE DATABASE WINDOWS_FUNCTION_REGRESS ENGINE=INFINIDB;

-- USE WINDOWS_FUNCTION_REGRESS;
drop table if exists tenk1;

CREATE TABLE tenk1 (
	unique1		int,
	unique2		int,
	two			int,
	four		int,
	ten			int,
	twenty		int,
	hundred		int,
	thousand	int,
	twothousand	int,
	fivethous	int,
	tenthous	int,
	odd			int,
	even		int,
	stringu1	varchar(100),
	stringu2	varchar(100),
	string4		varchar(100)
)engine=infinidb;

drop table if exists tenk2;

CREATE TABLE tenk2 (
	unique1 	int,
	unique2 	int,
	two 	 	int,
	four 		int,
	ten			int,
	twenty 		int,
	hundred 	int,
	thousand 	int,
	twothousand int,
	fivethous 	int,
	tenthous	int,
	odd			int,
	even		int,
	stringu1	varchar(100),
	stringu2	varchar(100),
	string4		varchar(100)
)engine=infinidb;

DROP TABLE if exists INT4_TBL;

CREATE TABLE INT4_TBL(f1 int)engine=infinidb;

INSERT INTO INT4_TBL(f1) VALUES ('   0  ');

INSERT INTO INT4_TBL(f1) VALUES ('123456     ');

INSERT INTO INT4_TBL(f1) VALUES ('    -123456');

INSERT INTO INT4_TBL(f1) VALUES ('34.5');

-- largest and smallest values
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');

INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');

-- bad input values -- should give errors
-- INSERT INTO INT4_TBL(f1) VALUES ('1000000000000');
-- INSERT INTO INT4_TBL(f1) VALUES ('asdf');
-- INSERT INTO INT4_TBL(f1) VALUES ('     ');
-- INSERT INTO INT4_TBL(f1) VALUES ('   asdf   ');
-- INSERT INTO INT4_TBL(f1) VALUES ('- 1234');
-- INSERT INTO INT4_TBL(f1) VALUES ('123       5');
-- INSERT INTO INT4_TBL(f1) VALUES ('');

drop table if exists empsalary3;

CREATE TABLE empsalary3 (
    depname varchar(100),
    empno bigint,
    salary int,
    enroll_date date
)engine=infinidb;

