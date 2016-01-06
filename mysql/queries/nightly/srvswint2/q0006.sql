-- Queries from shared nothing POC.

select now();
select now();
select calflushcache();

-- POC Q2.
-- q0006.1.d
Select sum(l_quantity), sum(l_extendedprice) from lineitem where l_quantity = 23 and l_shipdate <= '1993-01-30';
select calgetstats();
select now();

-- POC Q1
-- q0006.2.d
Select sum(l_quantity) from lineitem where l_quantity = 23 and l_shipdate <= '1993-01-30';
select calgetstats();
select now();

