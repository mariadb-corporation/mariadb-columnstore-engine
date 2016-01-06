drop table if exists rtrim_tbl;
drop table if exists rtrim_tbl_my;

CREATE TABLE `rtrim_tbl` (
  `col1` char(10) DEFAULT NULL,
  `col2` char(200) DEFAULT NULL
) ENGINE=InfiniDB DEFAULT CHARSET=utf8;

CREATE TABLE `rtrim_tbl_my` (
  `col1` char(10) DEFAULT NULL,
  `col2` char(200) DEFAULT NULL
) DEFAULT CHARSET=utf8;
