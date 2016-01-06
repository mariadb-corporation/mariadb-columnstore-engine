# 2013-02-06.  Commented out last three queries.  They are corner cases that no longer match with some changes we made to comply
# with open source licensing.

select conv(dt,  -10, -8) from dtypes where conv(dt, -10, -8) > 3731;  
select dtm from dtypes where conv(dtm, -10, -8) > 3731 order by conv(dtm, 10, 8);
select conv(db, 10, 16), conv(ti, 8,16), conv(si, 16, 8), conv(i, 4, 8), conv(bi, 10, 8) from dtypes;
select conv (c1, 5, 10), conv(substr(c8,2,4), 8, 10), conv(concat(vc1, vc2), 10,8) from dtypes;
select substr(vc255,2,3), conv(substr(vc255,2,3),16,10) from dtypes where id < 50 ;
#select conv(max(d182), 10, 20) from dtypes;
#select conv(bi, 10, 24) from dtypes where id < 20;
select conv (c1, 5, 2), conv(substr(c8,2,4), 8, 2), conv(concat(vc1, vc2), 10, 2) from dtypes;
drop table if exists bug3509;
Create table bug3509 (cookie varchar(32), d_datekey date) engine=infinidb;
insert into bug3509 values ('f48d2dce907ce3c54a9c12855754c0b5', 19980404);
select  conv(substr(cookie,1,12),16,10), conv(substr(cookie,1,16),16,10), conv(substr(cookie,1,16),18,10) from bug3509;
drop table bug3509;
