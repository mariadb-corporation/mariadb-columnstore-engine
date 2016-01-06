-- Validate 90,000 of the 150 million orders.  

select * from orders 
where 
round(o_orderkey,-4) + 1024 = o_orderkey or
round(o_orderkey,-4) + 2048 = o_orderkey or
round(o_orderkey,-4) + 4096 = o_orderkey or
round(o_orderkey,-4) + 6144 = o_orderkey or
round(o_orderkey,-4) + 8192 = o_orderkey
order by 1;
