\! rm -f /tmp/tmp.tbl

select *,
wide2.c19 a,
wide2.c19 b,
wide2.c19 c,
wide2.c19 d,
wide2.c19 e,
wide2.c19 f,
wide2.c19 g,
wide2.c19 h
into outfile '/tmp/tmp.tbl'
from wide2
where id <= 3000000;

\! wc -l /tmp/tmp.tbl
\! rm -f /tmp/tmp.tbl

