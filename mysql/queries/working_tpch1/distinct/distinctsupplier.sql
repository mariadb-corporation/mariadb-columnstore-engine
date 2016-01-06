select distinct * from supplier where s_suppkey < 100 order by 1,2;
select distinct s_suppkey from supplier where s_suppkey < 100 order by 1;
select distinct s_name   from supplier where s_suppkey < 100 order by 1;
-- ZZ. move this query to compareLogOnly because the case setting is different between the ref and infinidb.
-- select distinct s_address from supplier where s_suppkey < 100 order by 1;
select distinct s_nationkey from supplier where s_suppkey < 100 order by 1;
select distinct s_phone  from supplier where s_suppkey < 100 order by 1;
select distinct s_acctbal from supplier where s_suppkey < 100 order by 1;
select distinct s_comment from supplier where s_suppkey < 100 order by 1;

