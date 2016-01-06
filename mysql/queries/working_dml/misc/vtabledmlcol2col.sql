# WWW 5/13.  Wrapped l_orderkey/3.0 updates with round.  We truncate on updates where InnoDB rounds.
#Update with column=simple column
#
set autocommit=0;
update lineitem set l_partkey=l_orderkey where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
#update with multiple simple columns
#
set autocommit=0;
update lineitem set l_partkey=l_orderkey, l_returnflag=l_linestatus, l_comment = 'Hello world'  where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
#Update integer with decimal values
#
set autocommit=0;
# WWW - Changed 123.9 to 123.4 below to avoid difference between IDB (truncate) and InnoDB (round) behavior on updates.
update lineitem set l_orderkey=0.00009, l_partkey = 123.4 where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_suppkey, l_partkey, l_linenumber, l_tax;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
#update with multiple columns of different data types
#
set autocommit=0;
update lineitem set l_shipinstruct=l_linestatus, l_comment=l_shipmode where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
#Update with expression.  integer column with integer constant
#
set autocommit=0;
update lineitem set l_orderkey=round(l_orderkey / 3), l_partkey=l_partkey*3 where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_suppkey, l_partkey, l_linenumber;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
#Update with expression.  integer column with decimal constant
#
set autocommit=0;
update lineitem set l_orderkey=round(l_orderkey / 3.0), l_partkey=l_partkey*3.0 where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_suppkey, l_linenumber;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
#Update with expression. decimal column with integer constant
#
set autocommit=0;
update lineitem set l_quantity=round(l_quantity / 3), l_extendedprice=l_extendedprice*3 where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
#Update with expression. decimal column with decimal constant
#
set autocommit=0;
update lineitem set l_quantity=round(l_quantity / 3.0), l_extendedprice=l_extendedprice*3.0 where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
#Update with expression. Integer column with string value
#
set autocommit=0;
# WWW.  Changed 55.9 to 55.3 below to avoid truncate vs round differing behavior between IDB and InnoDB.
update lineitem set l_orderkey='55', l_partkey = '55.3' where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_suppkey, l_partkey, l_linenumber, l_tax;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
#Update with expression. Integer column with string value
#
set autocommit=0;
update lineitem set l_quantity='55', l_extendedprice = '55.9' where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
#update with functions and nested functions
#
set autocommit=0;
update lineitem set l_orderkey=length(l_comment) where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber, l_suppkey, l_partkey;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
set autocommit=0;
update lineitem set l_partkey=length(l_comment) * 10 where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
set autocommit=0;
update lineitem set l_partkey=power(length(l_comment) * 10,2) where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
set autocommit=0;
update lineitem set l_partkey=power(length(l_comment) * 10,2) - round(l_suppkey / l_quantity) where l_orderkey < 100;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_suppkey, l_linenumber, l_partkey;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;

#
# Update with joins
#
set autocommit=0;
update orders, customer set c_comment = o_clerk, c_acctbal=c_acctbal+o_totalprice where c_custkey = o_orderkey and c_custkey< 100;
select * from customer where c_custkey < 100 order by c_custkey;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
set autocommit=0;
update orders, customer set c_comment = o_clerk, c_acctbal=c_acctbal+o_totalprice where c_custkey = o_orderkey and o_custkey< 100;
select * from customer where c_custkey < 100 order by c_custkey;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
# update with query
#
set autocommit=0;
update orders, customer set c_comment = o_clerk, c_acctbal=c_acctbal+o_totalprice where c_custkey = o_orderkey and o_orderkey in (select l_orderkey from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber);
select * from customer where c_custkey < 100 order by c_custkey;
rollback;
select * from lineitem where l_orderkey < 100 order by l_orderkey, l_linenumber;
#
