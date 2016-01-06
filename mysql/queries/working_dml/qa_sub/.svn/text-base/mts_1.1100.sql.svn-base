drop table if exists t1;
drop table if exists t2;
create table t1 (a int) engine=infinidb;
insert into t1 values (1), (2), (3);
# WWW.  Edit below.  Wrapped the rand() call in an if so that it will compare with the reference.
# select a,(select (select rand() from t1 limit 1)  from t1 limit 1)from t1;
select a,(select (select if(rand()>0, 0, 1) from t1 limit 1) from t1 limit 1) xx from t1;

drop table t1;

CREATE TABLE t1 (ID int(11), name char(35), t2 char(3), District char(20), Population int(11)) ENGINE=infinidb;
INSERT INTO t1 VALUES (130,'Sydney','AUS','New South Wales',3276207);
INSERT INTO t1 VALUES (131,'Melbourne','AUS','Victoria',2865329);
INSERT INTO t1 VALUES (132,'Brisbane','AUS','Queensland',1291117);

-- Changed float(x,y)'s to floats to conform with IDB accepted syntax.
CREATE TABLE t2 (
  Code char(3),
  Name char(52),
  Continent char(15),
  Region char(26),
  SurfaceArea float,
  IndepYear smallint(6),
  Population int(11),
  LifeExpectancy float,
  GNP float,
  GNPOld float,
  LocalName char(45),
  GovernmentForm char(45),
  HeadOfState char(60),
  Capital int(11),
  Code2 char(2)
) ENGINE=infinidb;

INSERT INTO t2 VALUES ('AUS','Australia','Oceania','Australia and New Zealand',7741220.00,1901,18886000,79.8,351182.00,392911.00,'Australia','Constitutional Monarchy, Federation','Elisabeth II',135,'AU');
INSERT INTO t2 VALUES ('AZE','Azerbaijan','Asia','Middle East',86600.00,1991,7734000,62.9,4127.00,4100.00,'Azarbaycan','Federal Republic','Heyliyev',144,'AZ');

select t2.Continent, t1.Name, t1.Population from t2 LEFT JOIN t1 ON t2.Code = t1.t2  where t1.Population IN (select max(t1.Population) AS Population from t1, t2 where t1.t2 = t2.Code group by Continent); 

drop table t1;
drop table t2;

