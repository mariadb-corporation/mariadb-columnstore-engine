SELECT CONCAT_WS(',',n_name,r_name) func_concat,
       concat(r_name, n_name),
       greatest(r_name, n_name),
       least(r_name, n_name),
       lcase(n_name),
       left(n_name, 6) func_left,
       length(n_name),
       locate('HI', n_name),
       lower(n_name),
       lpad(n_name, 8, "*") func_lpad,
       position('HI' in n_name),
       replace(n_name, 'C', '*') func_replace,
       left(n_name, 6) func_right,
       rpad(n_name, 8, "*") func_rpad,
       space(r_regionkey) func_space,
       ucase(substr(n_comment,2,6)) func_substr              
       from nation, region 
       where r_regionkey = n_regionkey and n_nationkey=1;
       

