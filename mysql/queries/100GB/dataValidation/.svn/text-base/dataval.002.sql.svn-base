-- Validate that the customer name is on the right row for all customers.  Count should be 0.
select count(*) from customer where c_custkey != substr(c_name, 10, 99) + 0;
