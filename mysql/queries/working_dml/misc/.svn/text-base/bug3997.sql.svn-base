set autocommit=0;
update lineitem a join region b on b.r_regionkey = a.l_orderkey set b.r_regionkey=2;
select * from region;
rollback;
select * from region;
update region b join lineitem a on b.r_regionkey = a.l_orderkey set b.r_regionkey=2;
select * from region;
rollback;
select * from region;