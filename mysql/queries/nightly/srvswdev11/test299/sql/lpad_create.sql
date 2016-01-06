drop table if exists lpad_tbl;
drop table if exists lpad_tbl_my;

CREATE TABLE `lpad_tbl` (
  `col1` char(20) DEFAULT NULL,
  `col2` char(200) DEFAULT NULL
) ENGINE=InfiniDB DEFAULT CHARSET=utf8;

CREATE TABLE `lpad_tbl_my` (
  `col1` char(20) DEFAULT NULL,
  `col2` char(200) DEFAULT NULL
) CHARSET=utf8;
