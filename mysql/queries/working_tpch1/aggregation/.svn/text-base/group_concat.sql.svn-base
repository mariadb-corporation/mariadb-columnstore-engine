select group_concat(cchar1, cchar2, cchar3) from datatypetestm;
select group_concat(cchar1, cchar2, cchar3 order by 1, 2, 3) from datatypetestm;
select group_concat(cchar1, cchar2, cchar3 order by cchar1, cchar2, cchar3) gc from datatypetestm;
select group_concat(cchar1, '1', cchar2, '2', cchar3 order by cchar1, cchar2, cchar3) gc from datatypetestm;
select group_concat(cdouble,cchar1) from datatypetestm;

select group_concat(d12) from dtypes;
select group_concat(db, c1) from dtypes;

select group_concat(n_name order by n_regionkey) from nation;
select group_concat(n_name order by n_regionkey desc) from nation;
select group_concat(n_regionkey, n_name, n_nationkey) from nation;

select group_concat(n_regionkey, n_name, n_nationkey separator '') from nation;
select group_concat(n_nationkey, n_regionkey, n_name separator ':') gc from nation;
select group_concat(n_nationkey, n_regionkey, n_nationkey, n_name separator ':') gc from nation;
select group_concat(n_nationkey, n_regionkey, n_nationkey, n_name order by n_comment separator ':') gc from nation;
select group_concat(n_nationkey, n_regionkey, n_nationkey, n_name order by n_nationkey separator ':') gc from nation;
select group_concat(n_regionkey, n_name, n_nationkey order by n_comment separator ':') gc from nation;
select group_concat(n_regionkey, n_name, n_nationkey order by n_comment asc separator ':') gc from nation;
select group_concat(n_regionkey, n_name, n_nationkey order by n_comment desc separator ':') gc from nation;

select group_concat(r_regionkey) from region;
select group_concat(r_regionkey, r_name) from region;
select group_concat(r_name) from region;
select group_concat(r_name order by r_regionkey) from region;
select group_concat(r_name order by r_regionkey desc) from region;
select group_concat(r_name order by 1 desc separator '$') from region;
select group_concat(r_name order by r_regionkey desc separator '$') from region;

select group_concat(n_name, r_name order by 1) from nation, region where n_regionkey = r_regionkey;
select group_concat(r_name, n_name order by 2) from nation, region where n_regionkey = r_regionkey;
select group_concat(r_regionkey, '#', r_name, '@', n_nationkey, '&', n_name order by 1, 2, 3, 4, 5, 6, 7) gc from nation, region where n_regionkey = r_regionkey;

select group_concat(distinct n_regionkey order by 1) a from nation;
select group_concat(distinct n_regionkey order by 1 desc) a from nation;
select group_concat(distinct n_regionkey order by n_nationkey separator '$') a from nation;

select count(distinct n_regionkey), group_concat(n_nationkey, "=", n_name order by 1) from nation;
select count(distinct n_regionkey), count(distinct n_name), group_concat(n_nationkey, "=", n_name order by 1) from nation;
select count(distinct n_regionkey), count(distinct n_comment), count(distinct n_name), group_concat(n_nationkey, "=", n_name order by 1) from nation;
select count(distinct n_regionkey), count(distinct n_name), group_concat(n_nationkey, "=", n_name order by 1), group_concat(distinct n_comment, n_name) from nation;

select group_concat(distinct n_regionkey order by 1) a from nation;
select group_concat(distinct n_regionkey order by 1 desc) a from nation;
select group_concat(distinct n_regionkey order by n_nationkey separator '$') a from nation;

select count(distinct n_regionkey), group_concat(n_nationkey, "=", n_name order by 1) from nation;
select count(distinct n_regionkey), count(distinct n_name), group_concat(n_nationkey, "=", n_name order by 1) from nation;
select count(distinct n_regionkey), count(distinct n_comment), count(distinct n_name), group_concat(n_nationkey, "=", n_name order by 1) from nation;
select count(distinct n_regionkey), count(distinct n_name), group_concat(n_nationkey, "=", n_name order by 1), group_concat(distinct n_comment, n_name order by 2) from nation;

-- bug3756
select group_concat('hello') from datatypetestm;
select group_concat('hello, world') from datatypetestm;
select group_concat(1) from datatypetestm;
select group_concat('2001-01-01') from datatypetestm;
select group_concat(1,2) from datatypetestm;

set max_length_for_sort_data=2048;
select cidx, cinteger, group_concat(cinteger,cchar8,'calpont' order by cinteger desc) from datatypetestm group by cidx, cinteger order by cidx;
select cidx, cinteger, group_concat(cinteger,cchar8,'calpont' order by cdate desc) from datatypetestm group by cidx, cinteger order by cidx;

select cidx, cinteger, count(distinct cinteger), group_concat(cinteger,cchar8,'calpont' order by cinteger desc) from datatypetestm group by cidx, cinteger order by cidx;

-- bug3839
select group_concat(PN) from (select p_name PN from part where p_partkey < 4) a;

-- bug3921
select length(group_concat(r_regionkey)) from region;
select length(group_concat(r_name)) from region;
select length(group_concat(r_regionkey, r_name)) from region;
select concat(group_concat(r_name), group_concat(r_name)) from region;

-- bug4397
select count(*) from (select o_orderkey, group_concat(o_comment) from orders where o_orderkey <= 1000000 group by 1) a ;
select count(*) from (select o_orderkey, group_concat(o_comment) from orders where o_orderkey <= 1000000 group by 1) a ;

