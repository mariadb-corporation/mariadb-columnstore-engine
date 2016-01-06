select n_nationkey, group_concat(c_name) from customer, nation where n_nationkey = c_nationkey group by n_nationkey;
select r_regionkey, group_concat(c_name) from customer, nation, region where n_nationkey = c_nationkey and r_regionkey = n_regionkey group by r_regionkey;

select group_concat(l_comment) from lineitem;
select group_concat(l_linenumber) from lineitem;

select group_concat(distinct n_regionkey) a from nation;
select count(distinct n_regionkey), group_concat(n_nationkey, "=", n_name) from nation;
select n_regionkey, count(distinct n_regionkey), count(distinct n_name), group_concat(n_nationkey, "=", n_name order by 1) from nation group by 1;
select n_regionkey, count(distinct n_regionkey), count(distinct n_name), group_concat(n_nationkey, "=", n_name order by 1), group_concat(n_name) from nation group by 1;
select n_regionkey, count(distinct n_regionkey), count(distinct n_name), group_concat(n_nationkey, "=", n_name order by 1), group_concat(distinct n_comment, n_name) from nation group by 1;
select n_regionkey, count(distinct n_nationkey), count(distinct n_comment), group_concat(n_nationkey, "=", n_name order by 1), group_concat(n_name) from nation group by 1;
select n_regionkey, count(distinct n_regionkey), count(distinct n_comment), group_concat(n_nationkey, "=", n_name order by 1), group_concat(n_name) from nation group by 1;
select n_regionkey, count(distinct n_regionkey), count(distinct n_comment), count(distinct n_name), group_concat(n_nationkey, "=", n_name order by 1) from nation group by 1;

