drop table if exists food;
drop table if exists animals;
drop table if exists testsum;

create table food (
id int,
food char(16),
type char(16),
animal char(16)
) ENGINE=InfiniDB DEFAULT CHARSET=latin1;

create table animals (
type char(16),
animal char(16),
date_id int,
weight int
) ENGINE=InfiniDB DEFAULT CHARSET=latin1;

insert into food values
(1, 'meat', 'mammal','dog'),
(2, 'fish', 'mammal','cat'),
(3, 'insects', 'bird','sparrow');

insert into animals values
('mammal', 'cat', 14173, 6),
('mammal', 'cat', 14174, 30),
('mammal', 'cat', 14182, 12),
('insects', 'bird', 14182, 5);

select food.food, food.animal, if (ifnull(SUM(weight),0) > 0,1,99) as test1, 
if (ifnull(SUM(weight),0) > 0 && ifnull(COUNT(*),0) > 0, CONCAT('1:', ROUND(SUM(weight) / COUNT(*),1)),'n/a') as 'Ratio Test' 
from food LEFT OUTER JOIN (select type, animal, sum(weight) as weight, count(*) as counter from animals group by type, animal) meal 
using (type, animal) where food.type='mammal' group by food.food, food.animal order by 1, 2, 3, 4;

select animal, if(sum(weight)>1,1,0) as 'test1' from animals group by animal order by 1, 2;

select food.food, food.animal, if (ifnull(SUM(weight),0) > 0,1,99) as test1,
if (ifnull(SUM(weight),0) > 0 && ifnull(COUNT(*),0) > 0, CONCAT('1:', ROUND(SUM(weight) / COUNT(*),1)),'n/a') as 'Ratio Test' 
from food LEFT OUTER JOIN animals meal using (type, animal) where food.type='mammal' group by food.food, 
food.animal order by 1, 2, 3, 4;

select if (animals.animal = 'cat', animals.type, concat('Private',animals.date_id)) as 'Test',
animals.animal, animals.weight, animals.date_id from (select type, animal from food where type='mammal') f 
join animals on (animals.animal = f.animal) order by 1,2,3,4;


create table testsum (id int, testcol tinyint(1)) ENGINE=InfiniDB DEFAULT
CHARSET=latin1;

insert into testsum values (1,1);
insert into testsum values (2,2);
select IF(testcol>0, 1, 0) as 'TestSum1' from testsum order by 1;
select sum(if(testcol>0 and testcol<1000, 1, 0)) as '0-1000' from testsum order by 1;
select IF(testcol, 1, 0) as 'TestSum1' from testsum order by 1;

drop table food;
drop table animals;
drop table testsum;

select  IF(  ( IF(r_regionkey >= 0x40000000, r_regionkey+10, r_regionkey) > 0 ) , 1, 0) page from region cc order by 1 ;
