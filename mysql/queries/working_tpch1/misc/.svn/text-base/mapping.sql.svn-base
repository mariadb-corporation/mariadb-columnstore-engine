-- bug4934
select r_regionkey, r_regionkey from (select r_regionkey from region) r;

-- bug4935
select o_custkey as user, max(o_orderdate) as date, max(o_totalprice) as max_level, count(distinct o_totalprice) as nb_level_up, (max(o_totalprice) - count(distinct(o_totalprice))) as diff_level from orders where o_custkey < 10 group by user order by user;

