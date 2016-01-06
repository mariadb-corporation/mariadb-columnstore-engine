-- Edits.  Changed xdate column name to xxdate.
drop table if exists t1;
drop table if exists t2;

CREATE TABLE `t1` (
  `topic` int(8), `xdate` date, `pseudo` varchar(35)) ENGINE=infinidb;

INSERT INTO t1 (topic,xdate,pseudo) VALUES ('43506','2002-10-02','joce'),('40143','2002-08-03','joce');

SELECT DISTINCT xdate FROM t1 WHERE xdate='2002-08-03';
SELECT (SELECT DISTINCT xdate FROM t1 WHERE xdate='2002-08-03');
# ERROR 138 (HY000) at line 12: IDB-3013: Subquery with union is currently only supported in the FROM clause.
# SELECT 1 FROM t1 WHERE 1=(SELECT 1 UNION SELECT 1) UNION ALL SELECT 1;
drop table t1;


CREATE TABLE `t1` (`numeropost` int(8), `maxnumrep` int(10)) ENGINE=infinidb;
INSERT INTO t1 (numeropost,maxnumrep) VALUES (40143,1),(43506,2);

CREATE TABLE `t2` (`mot` varchar(30), `topic` int(8), `xdate` date, `pseudo` varchar(35)) ENGINE=infinidb;
INSERT INTO t2 (mot,topic,xdate,pseudo) VALUES ('joce','40143','2002-10-22','joce'), ('joce','43506','2002-10-22','joce');

# ERROR 138 (HY000) at line 22: IDB-2016: Non supported item on the GROUP BY list.
# select numeropost as a FROM t1 GROUP BY (SELECT 1 FROM t1 HAVING a=1);
SELECT numeropost,maxnumrep FROM t1 WHERE exists (SELECT 1 FROM t2 WHERE (mot='joce') AND xdate >= '2002-10-21' AND t1.numeropost = t2.topic) ORDER BY maxnumrep DESC LIMIT 0, 20;

SELECT * from t2 where topic IN (SELECT topic FROM t2 GROUP BY topic);
SELECT * from t2 where topic IN (SELECT topic FROM t2 GROUP BY topic HAVING topic < 4100);
# ERROR 138 (HY000) at line 27: IDB-1000: 't2' is not joined.
# SELECT * from t2 where topic IN (SELECT SUM(topic) FROM t1);
SELECT * from t2 where topic IN (SELECT topic FROM t2 GROUP BY topic HAVING topic < 41000);
drop table t1;
drop table t2;

