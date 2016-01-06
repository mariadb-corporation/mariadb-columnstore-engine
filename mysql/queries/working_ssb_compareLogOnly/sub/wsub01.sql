select count(*) from part where p_partkey in (select sum(c_custkey) from customer where c_custkey in (select sum(lo_linenumber)from lineorder group by lo_orderdate) group by c_city);

select count(*) from part, lineorder where p_partkey = lo_partkey and p_partkey in (select sum(c_custkey) from customer where c_custkey in (select sum(lo_linenumber)from lineorder group by lo_orderkey) group by c_city);

select c_custkey from part, lineorder right join customer on lo_custkey = c_custkey where p_partkey = lo_partkey and p_partkey in (select sum(c_custkey) from customer where c_custkey in (select sum(lo_linenumber)from lineorder group by lo_orderkey) group by c_city) group by c_custkey order by c_custkey;

select c_custkey from part, lineorder right join customer on lo_custkey = c_custkey where p_partkey = lo_partkey and p_partkey in (select sum(c_custkey) from customer where c_custkey in (select sum(lo_linenumber)from lineorder group by lo_orderkey) group by c_city) order by c_custkey;

