drop table if exists t1;
drop table if exists t2;
drop table if exists t1xt2;

CREATE TABLE t1 (
  id_1 int(5) ,
  t varchar(4)
) engine=infinidb;

CREATE TABLE t2 (
  id_2 int(5),
  t varchar(4)
) engine=infinidb;

CREATE TABLE t1xt2 (
  id_1 int(5),
  id_2 int(5)
) engine=infinidb;

INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

INSERT INTO t2 VALUES (2, 'bb'), (3, 'cc'), (4, 'dd'), (12, 'aa');

INSERT INTO t1xt2 VALUES (2, 2), (3, 3), (4, 4);

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)))) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 NOT IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 NOT IN ((SELECT t1xt2.id_2 FROM t1xt2 where t1.id_1 = t1xt2.id_1))) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 NOT IN (((SELECT t1xt2.id_2 FROM t1xt2 where t1.id_1 = t1xt2.id_1)))) order by 1;

insert INTO t1xt2 VALUES (1, 12);
SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)))) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 NOT IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 NOT IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 NOT IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)))) order by 1;

insert INTO t1xt2 VALUES (2, 12);

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)))) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 NOT IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 NOT IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))) order by 1;

SELECT DISTINCT t1.id_1 FROM t1 WHERE
(12 NOT IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)))) order by 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t1xt2;

