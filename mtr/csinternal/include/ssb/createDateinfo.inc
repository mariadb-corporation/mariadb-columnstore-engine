# create dateinfo table
#
--disable_warnings
drop table if exists dateinfo;
--enable_warnings
create table `dateinfo` (
        d_datekey int,
        d_date char (18),
        d_dayofweek char (9),
        d_month char (9),
        d_year int,
        d_yearmonthnum int,
        d_yearmonth char (7),
        d_daynuminweek int,
        d_daynuminmonth int,
        d_daynuminyear int,
        d_monthnuminyear int,
        d_weeknuminyear int,
        d_sellingseason varchar (12),
        d_lastdayinweekfl tinyint,
        d_lastdayinmonthfl tinyint,
        d_holidayfl tinyint,
        d_weekdayfl tinyint
) engine=Columnstore;
SHOW CREATE TABLE dateinfo;
