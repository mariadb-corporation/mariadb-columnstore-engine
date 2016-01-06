drop table if exists ltrim_tbl;
drop table if exists ltrim_tbl_my;

CREATE TABLE `ltrim_tbl` (
  `col1` char(10) DEFAULT NULL,
  `col2` char(200) DEFAULT NULL
) ENGINE=InfiniDB DEFAULT CHARSET=utf8;

CREATE TABLE `ltrim_tbl_my` (
  `col1` char(10) DEFAULT NULL,
  `col2` char(200) DEFAULT NULL
) DEFAULT CHARSET=utf8;
