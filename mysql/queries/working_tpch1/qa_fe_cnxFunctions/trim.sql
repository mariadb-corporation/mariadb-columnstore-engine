-- bug 3371
select trim(leading 'A' from r_name) from region;
select trim(trailing 'A' from r_name) from region;
select trim(both 'A' from r_name) from region;
select r_name from region where trim(both 'A' from r_name) = 'SI';


