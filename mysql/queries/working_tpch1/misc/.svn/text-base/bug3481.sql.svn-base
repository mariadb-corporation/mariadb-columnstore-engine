set autocommit = 0;
select cidx,cbigint,cdecimal9, cast(cbigint as decimal(9,2)) from datatypetestm;
update datatypetestm set cdecimal9=cast(cbigint as decimal(9,2));
select cidx,cbigint,cdecimal9, cast(cbigint as decimal(9,2)) from datatypetestm;
rollback;
update datatypetestm set cidx=cidx*10, CDECIMAL10=CAST(CBIGINT AS DECIMAL(9,2)) where CAST(CBIGINT AS DECIMAL(9,2)) > 
0;
select CDECIMAL10 from datatypetestm order by cidx;
rollback;
update datatypetestm set cidx=cidx*10, CDECIMAL18=CAST(CBIGINT AS DECIMAL(9,2)) where CAST(CBIGINT AS DECIMAL(9,2)) > 
0;
select CDECIMAL18 from datatypetestm order by cidx;
rollback;
update datatypetestm set cidx=cidx*10, CDECIMAL18_2=CAST(CBIGINT AS DECIMAL(9,2)) where CAST(CBIGINT AS DECIMAL(9,2)) 
> 0;
select CDECIMAL18_2 from datatypetestm order by cidx;
rollback;
update datatypetestm set cidx=cidx*10, CINTEGER=CAST(CBIGINT AS DECIMAL(9,2)) where CAST(CBIGINT AS DECIMAL(9,2)) > 
0;
select CINTEGER from datatypetestm order by cidx;
rollback;

drop table if exists gen;
drop table if exists casttb;
create table gen (i int, i2 int) engine=infinidb;
create table casttb(c1 int, c2 int)engine=infinidb;
insert into gen values
(1,0),(1,1),(1,2),(1,3),(1,4),(1,5),(1,6),(1,7),(1,8),(1,9);

set @rn2 := -1000000;
set @rn3 := -100;

insert into casttb
select  @rn2:=@rn2 + 10000,
    @rn3:=@rn3 + 1
from (select g1.i2 a, g2.i2 b 
    from gen g1  
        join gen g2 using(i) 
        join gen g3 using(i) 
     ) a  limit 200;

update casttb set c2=cast(c1 as decimal(9,2));
select * from casttb;
update casttb set c2=cast(c1 as decimal(5,3));
select * from casttb;
update casttb set c2=cast(c1 as decimal(5));
select * from casttb;
drop table gen;
drop table casttb;
