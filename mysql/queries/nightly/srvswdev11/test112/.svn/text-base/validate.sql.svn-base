# Validation sql statement.

select count(*) from test012_fact where c3 is null;

select count(*) from test012_fact where c3 is not null;

select id, hex(c3) from test012_fact where id = 2 union 
select id, hex(c1) from test012_staging where id = 2;

select id, hex(c3) from test012_fact where id = 2 union 
select id, hex(c1) from test012_staging where id = 2;

select id, hex(c1) from test012_staging where id in (1, 3, 5);
select id, hex(c3) from test012_fact where id in (200, 205, 999);
