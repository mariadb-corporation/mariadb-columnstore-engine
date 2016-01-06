select * into @a1, @a2, @a3 from region where r_regionkey = 1;
select * from region where r_regionkey = @a1;
select * from region where r_regionkey > @a1 order by r_regionkey;

