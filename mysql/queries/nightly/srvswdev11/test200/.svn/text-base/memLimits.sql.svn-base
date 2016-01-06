/* Boundary tests for TotalUMMemory at 4G. */

/* At the limit. Four byte count distinct.  Not exactly the same every time w/ multi-threaded distinct. */

select count(c1) as count1 from test200 where c1 <= 130000000;
select count(distinct c1) as count2 from test200 where c1 <= 130000000;
select sleep(30);

/* One over the limit, IDB error expected. */
select count(distinct c1) as count3 from test200 where c1 <= 150000000;
select sleep(30);

/* At the limit.  Four byte column join.  Note:  Limit is two less rows on perf stack than single node stacks.  */
select count(a.c1) as count4 from test200 a where a.c1 <= 100761020; 
select count(a.c1) as count5 from test200 a join test200 b on a.c1 = b.c1 where a.c1 <= 100761020;
select sleep(30);

/* One over the limit.  IDB error expected. */
select count(a.c1) as count6 from test200 a join test200 b on a.c1 = b.c1 where a.c1 <= 100761023;
select sleep(30);

/* At the limit. Eight byte column join.  Note:  Limit is two less rows on perf stack than single node stacks.*/
select count(a.c2) as count7 from test200 a where a.c2 <= 88907548;
select count(a.c2) as count8 from test200 a join test200 b on a.c2 = b.c2 where a.c2 <= 88907548;
select sleep(30);

/* One over the limit.  IDB error expected. */
select count(a.c2) as count9 from test200 a join test200 b on a.c2 = b.c2 where a.c2 <= 88907551;
select sleep(30);

/* At the limit.  Four byte column union.  Not an exact limit on this one, have a low priority bug 3465 open to look into this at some point.  */
select count(*) as count10  from test200 where c1 <= 136000000;
select count(*) as count11 from (select c1 from test200 where c1 <= 136000000 union select c1 from test200 where c1 <= 1000000) x;
select sleep(30);

/* Over the limit.  IDB error expected. */
select count(*) as count12 from (select c1 from test200 where c1 <= 140000000 union select c1 from test200 where c1 <= 1000000) x;
select sleep(60);

/* At the limit.  Group by both cols.  Not an exact limit on this one either. */
/* Two runs with trunk nightly on 2013/03/28 capped out at 68696903 and 68703379. */
select count(*) count13 from test200 where c1 <= 68000000;
select F, count(*) as count14 from (select count(*) F from test200 where c1 <= 68000000 group by c1, c2) a group by 1;
select sleep(30);

/* One over the limit.  IDB error expected. */
select F, count(*) as count15 from (select count(*) F from test200 where c1 <= 70000000 group by c1, c2) a group by 1;
select sleep(30);

/* Update statement */
/* NOTE:  This is currently taking a very long time to run.  The real limit is something up in the 88 million range.  I'm going with 80 mil here for now until we get the performance improved. */
update test200 x set x.c2=(select sub.c1 from test200b sub where sub.c1 <= 70000000 and sub.c1 = x.c1) where x.c1 <= 70000000;

/* Over the limit.  IDB error expected. */
update test200 x set x.c2=(select sub.c1 from test200b sub where sub.c1 <= 100000000 and sub.c1 = x.c1) where x.c1 <= 100000000;

/* At the limit.  Join on varchar(1000) column. */
select count(*) as count16 from test200 a join test200 b using (c3) where a.c1 <= 51372630;

/* One over the limit.  Join on varchar(1000) column. */
select count(*) as count16 from test200 a join test200 b using (c3) where a.c1 <= 51372632;
