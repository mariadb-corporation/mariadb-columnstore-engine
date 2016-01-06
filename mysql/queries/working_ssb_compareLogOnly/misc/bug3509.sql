# 2013-02-06.  Commented out first two queries.  They are corner cases that no longer match with changes made for license compliance.
#select 'A' A, conv(d_datekey,10,2), 'B' B from dateinfo where d_datekey = 19980404;
#select 'A' A,MD5(d_datekey), 'B' B from dateinfo where d_datekey = 19980404;
select 'A' A,REVERSE(d_datekey), 'B' B from dateinfo where d_datekey = 19980404;
select 'A' A,REVERSE(d_datekey), md5(d_datekey) from dateinfo where d_datekey = 19980404;

