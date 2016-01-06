drop table if exists ifelse_tbl;
drop table if exists ifelse_tbl_my;

CREATE TABLE `ifelse_tbl` (
  `col1` char(20) DEFAULT NULL,
  `col2` char(200) DEFAULT NULL
) ENGINE=InfiniDB DEFAULT CHARSET=utf8;

CREATE TABLE `ifelse_tbl_my` (
  `col1` char(20) DEFAULT NULL,
  `col2` char(200) DEFAULT NULL
) CHARSET=utf8;

