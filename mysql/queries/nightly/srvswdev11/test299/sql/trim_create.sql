drop table if exists trim_tbl;
drop table if exists trim_tbl_my;

CREATE TABLE `trim_tbl` (
  `col1` char(10) DEFAULT NULL,
  `col2` char(200) DEFAULT NULL
) ENGINE=InfiniDB DEFAULT CHARSET=utf8;

CREATE TABLE `trim_tbl_my` (
  `col1` char(10) DEFAULT NULL,
  `col2` char(200) DEFAULT NULL
) DEFAULT CHARSET=utf8;
