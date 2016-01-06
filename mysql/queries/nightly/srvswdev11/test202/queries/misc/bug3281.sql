\! rm -f /tmp/tmp.tbl
select 
format(c1, 2) c1,
format(c2, 2) c2,
format(c3, 2) c3,
format(c4, 2) c4,
format(c5, 2) c5,
format(c6, 2) c6,
format(c7, 2) c7,
format(c8, 2) c8,
format(c1, 2) a,
format(c2, 2) b,
format(c3, 2) c,
format(c4, 2) d,
format(c5, 2) e,
format(c6, 2) f,
format(c7, 2) g,
format(c8, 2) h
into outfile '/tmp/tmp.tbl'
from wide;

\! wc -l /tmp/tmp.tbl
\! rm -f /tmp/tmp.tbl

