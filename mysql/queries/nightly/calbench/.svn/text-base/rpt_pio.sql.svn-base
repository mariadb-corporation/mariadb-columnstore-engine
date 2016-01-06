
-- ----------------------------------------------
-- PIO Scalability
-- ----------------------------------------------
  select RunDesc, description, 
	 substr(sqltext,1,20),
	 pm_count PMs,
	 round(elapsedtime,2) ela,
	 physical_IO PIO,
	 round(physical_io/pm_count/elapsedtime,0) "PIO/Sec/PM",
	 round(physical_io/pm_count/elapsedtime/512,2) "pct_tgt",
	 case when round(physical_io/pm_count/elapsedtime,0)
		  < (51200 * .65) then 'Fail vs. 65% of FC Limit' 
		else '      Pass' end	
	 "Pass/Fail"
  from 	 run_hist 
 where 	 starttime > now()-interval 3 day
	 and rundesc = @rundesc and description = 'PIO Scalability'
order by id;


-- ----------------------------------------------
-- PIO Scalability - Different PM CASES
-- ----------------------------------------------
  select RunDesc, description, 
	 pm_count PMs,
	 round(avg(physical_io/pm_count/elapsedtime),0) "Average PIO/Sec/PM",
	 round(avg(physical_io/pm_count/elapsedtime)/512,2) "pct_target",
	 case when round(avg(physical_io/pm_count/elapsedtime),0)
		  < (51200 * .70) then 'Fail vs. 70% of FC Theoretical' 
		else '      Pass' end	
	 "Pass/Fail"
  from 	 run_hist 
 where 	 rundesc = @rundesc and description = 'PIO Scalability'
group by 1,2,3
order by 1,2,3;



-- ----------------------------------------------
-- PIO Scalability - Overall CASES
-- ----------------------------------------------
  select RunDesc, description, 
	 round(avg(physical_io/pm_count/elapsedtime),0) "Average PIO/Sec/PM",
	 round(avg(physical_io/pm_count/elapsedtime)/512,2) "pct_target",
	 case when round(avg(physical_io/pm_count/elapsedtime),0)
		  < (51200 * .70) then 'Fail vs. 65% of FC Theoretical' 
		else '      Pass' end	
	 "Pass/Fail"
  from 	 run_hist 
 where 	 rundesc = @rundesc and description = 'PIO Scalability'
group by 1,2
order by 1,2;



-- ----------------------------------------------
-- PIO Repeatability
-- ----------------------------------------------
  select RunDesc, substr(sqltext,1,25), description, 
	 count(*) cnt,
	 round(avg(elapsedtime),2) Avg_Ela,	
	 round(min(elapsedtime),2) Min_Ela,	
	 round(max(elapsedtime),2) Max_Ela,
	 round(std(elapsedtime)/avg(elapsedtime),4) "Std/Avg_Ela",
	 case when round(std(elapsedtime)/avg(elapsedtime),4) > 0.05 
		then 'Fail w/ Std/Avg_Ela > 0.05'
		else '      Pass' end "Pass/Fail"
  from 	 run_hist 
 where 	 rundesc = @rundesc and description = 'PIO Repeat'
group by 1,2,3
order by 1,2,3;


-- ----------------------------------------------
-- PIO Concurrency
-- ----------------------------------------------
  select rundesc, description, concurrent CXs, pm_count PMs,
	 sum(physical_io) total_PIO,
 	 round(sum(elapsedtime)/concurrent,2) Avg_Sess_Ela,
 	 time_to_sec(timediff(max(starttime + interval elapsedtime second) ,min(starttime))) Max_Sess_Ela,	 
	round(sum(physical_io)/(sum(elapsedtime)/concurrent)/pm_count,0) "Blks/Seconds/PM",
	 case when sum(physical_io)/(sum(elapsedtime)/concurrent)/pm_count
		  < (51200 * .70) then 'Fail vs. 65% of FC Theoretical' 
		else '      Pass' end	
	 "Pass/Fail"
  from 	 run_hist 
 where 	 rundesc = @rundesc and
	 description like 'PIO Concurrency%'
	 and stmtdesc <> 'no-op'
group by rundesc, description, concurrent, pm_count
order by rundesc, description, concurrent, pm_count;

