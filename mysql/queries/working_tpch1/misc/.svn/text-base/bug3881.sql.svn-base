set @a:=(select max(r_regionkey) from region);
select @a from region;
select @a:=@a+1 from region;
select @a, n_regionkey from nation;
select @a, n_regionkey from nation group by n_regionkey order by 1,2;

-- Note: the following two queries have different behavior than the reference. We applied the
-- assignment on the output row to give an auto-increment value, which is what eyeWonder needs.
-- MySQL ref applies the assignment on the input rows, and should really error this query out
-- for non-group by item.
select @a:=@a+1, n_regionkey from nation group by 2 order by 2;
select @a:=@a+2 as c1, n_regionkey as c2, 1 as c3, '' as c4 from nation group by c2 order by 2;

