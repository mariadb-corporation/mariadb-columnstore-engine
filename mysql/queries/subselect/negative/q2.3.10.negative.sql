-- Invalid/not supported Equality Variant cases that use <> (possible Cartesian product cases):
Select r_name from region where r_name <> ( select max(n_name) from nation where r_regionkey <> n_regionkey  );
Select r_name from region where r_name <> ( select max(n_name) from nation where r_regionkey > n_regionkey  );

