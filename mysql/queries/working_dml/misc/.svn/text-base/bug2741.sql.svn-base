set autocommit=0;
select count(*) from nation where n_nationkey * (n_regionkey > 0) = 0;
select count(*) from nation where n_nationkey * (n_regionkey > 0) = 0;
select count(*) from nation where n_nationkey * (n_regionkey > 0) = 0;
select count(*) from nation where n_nationkey * (n_regionkey > 2) = 0;
select count(*) from nation where n_nationkey * (n_regionkey > 2) = 0;
select count(*) from nation where n_nationkey * (n_regionkey > 2) = 0;
update nation set n_regionkey = n_nationkey where n_nationkey * (n_regionkey > 0) = 0;
select count(*) from nation where n_nationkey * (n_regionkey > 0) = 0;
select * from nation;
rollback;
update nation set n_regionkey = n_nationkey where n_nationkey * (n_regionkey > 2) = 0;
select count(*) from nation where n_nationkey * (n_regionkey > 2) = 0;
select * from nation;
rollback;

