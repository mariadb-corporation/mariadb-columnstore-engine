-- negative test
select n_nationkey xor n_regionkey from nation;

select * from nation where n_nationkey > 4 xor n_nationkey < 6;
select * from nation where n_nationkey xor n_regionkey or n_nationkey+1;
select n_regionkey from nation where n_regionkey < 4 xor (n_name>'B' and n_nationkey < 10);
select * from nation where n_nationkey > 10 xor n_nationkey < 11 and n_regionkey > 2;
select * from nation where (n_nationkey > 10 xor n_nationkey < 11) and n_regionkey > 2;
select n_regionkey, n_name from region, nation where r_regionkey=n_regionkey and (n_nationkey < 20 xor n_nationkey > 10 xor n_regionkey between 1 and 3) order by 1, 2;
select n_regionkey, n_nationkey from nation where n_nationkey > 10 xor n_regionkey xor n_name xor n_comment;
select * from region where r_regionkey < (select max(n_regionkey) from nation where n_nationkey < 4 xor n_nationkey > 10) order by 1, 2;
select count(*) from nation group by n_regionkey having sum(n_nationkey) > 20 xor avg(n_nationkey) < 15;

