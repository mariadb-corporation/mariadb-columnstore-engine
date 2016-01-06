
-- ----------------------------------------------
-- Display Distinct Error Messages
-- ----------------------------------------------
select rundesc into @last_rundesc from run_hist where id = (select max(id) from run_hist);

-- ----------------------------------------------
-- PIO Scalability
-- ----------------------------------------------
  select description, 
	 case when round(physical_io/pm_count/elapsedtime,0)
		  < (51200 * .70) then 'Fail vs 70% of FC Limit' 
		else 'Pass' end	
	 "Pass/Fail",
	 substr(sqltext,1,30),
	 pm_count PMs,
	 round(elapsedtime,2) ela,
	 physical_IO PIO,
	 round(physical_io/pm_count/elapsedtime,0) "PIO/Sec/PM",
	 round(physical_io/pm_count/elapsedtime/512,2) "pct_FC",
	RunDesc, id
  from 	 run_hist 
 where 	 starttime > now()-interval 3 day
	 and rundesc = @last_rundesc and description = 'PIO Scalability'
order by sqltext, pm_count;


-- ----------------------------------------------
-- PIO Scalability - Different PM CASES
-- ----------------------------------------------
  select description, 
	 case when round(avg(physical_io/pm_count/elapsedtime),0)
		  < (51200 * .70) then 'Fail vs 70% of FC Theoretical' 
		else 'Pass' end	
	 "Pass/Fail",
	 pm_count PMs,
	 round(avg(physical_io/pm_count/elapsedtime),0) "Average PIO/Sec/PM",
	 round(avg(physical_io/pm_count/elapsedtime)/512,2) "pct_target",
 	 RunDesc
  from 	 run_hist 
 where 	 starttime > now()-interval 3 day
	 and rundesc = @last_rundesc and description = 'PIO Scalability'
group by description, pm_count, rundesc
order by description, pm_count, rundesc;


-- ----------------------------------------------
-- PIO Scalability - Overall CASES
-- ----------------------------------------------
  select description, 
	 case when round(avg(physical_io/pm_count/elapsedtime),0)
		  < (51200 * .70) then 'Fail vs 70% of FC Theoretical' 
		else 'Pass' end	
	 "Pass/Fail",
	 round(avg(physical_io/pm_count/elapsedtime),0) "Average PIO/Sec/PM",
	 round(avg(physical_io/pm_count/elapsedtime)/512,2) "pct_target",
	 RunDesc
  from 	 run_hist 
 where 	 starttime > now()-interval 3 day
	 and rundesc = @last_rundesc and description = 'PIO Scalability'
group by description, rundesc
order by description, rundesc;



