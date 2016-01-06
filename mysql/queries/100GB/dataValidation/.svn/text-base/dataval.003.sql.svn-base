-- Validate that the supplier name is on the right row for all suppliers.  Count should be 0.
select count(*) from supplier where s_suppkey != substr(s_name, 10, 99) + 0;
