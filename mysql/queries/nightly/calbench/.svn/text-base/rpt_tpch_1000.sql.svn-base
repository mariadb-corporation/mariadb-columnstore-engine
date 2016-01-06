
select rundesc into @last_rundesc from run_hist where id = (select max(id) from run_hist where rundesc is not null);


-- -------------------------------------------------------------
-- Report absolute elapsed
-- -------------------------------------------------------------
select 	 
	 r.stmtdesc Qry, r.pm_count PMs, r.sequence Run, 
	 round(r.elapsedtime,2) Elapsed,  
	 round(t.elapsedtime,0) "P/P 2x Tgt",  
	 round(t.elapsedtime *2,0) "Trend Line",  
	 case when r.elapsedtime > (t.elapsedtime  *2) +.1
		then concat('Fail: ',round(r.elapsedtime/(t.elapsedtime*2),2),' vs Trend Line') 
	      when r.elapsedtime > (t.elapsedtime) +.1
		then concat('      Meet: ',round(r.elapsedtime/(t.elapsedtime*2),2),' vs Trend Line') 
	      else 
		concat('            Exceed: ',round(r.elapsedtime/(t.elapsedtime),2),' vs 2x Target') 
	      end  "vs Price/Performance Trend Line"
   from  run_hist  r, run_hist_target t
  where  r.starttime > now()-interval 3 day  
    and  r.rundesc = @rundesc
    and  r.description = 'ACB_4_tpch_1t'
    and  r.stmtdesc <> 'no-op' 
    and  r.description = t.description
    and  r.stmtdesc = t.stmtdesc
    and  r.sequence = t.sequence
    and  r.pm_count = t.pm_count
order by  r.stmtdesc, r.pm_count, r.sequence;

-- -------------------------------------------------------------
-- Report absolute elapsed - overall
-- -------------------------------------------------------------
select 	 
	 round(avg(r.elapsedtime),2) Avg_Elapsed,  
	 round(avg(t.elapsedtime),0) "P/P 2x Tgt",  
	 round(avg(t.elapsedtime)*2,0) "Trend Line",  
	 round(avg(r.elapsedtime)/avg(t.elapsedtime) ,2) "Avg(Ela)/Avg(2x Tgt)",
	 round(avg(r.elapsedtime/(t.elapsedtime)),2) "Avg(Ela/2x Tgt)"  /* ,
		 case when avg(r.elapsedtime/(t.elapsedtime )) <= .5		
			then concat('Exceed: ',round(avg(r.elapsedtime/(t.elapsedtime )),2),' vs 2x Tgt') 
		      when avg(r.elapsedtime/(t.elapsedtime )) <= 1.0		
			then concat('Meet: ',round(avg(r.elapsedtime/(t.elapsedtime )),2),' vs 2x Tgt') 
		      when avg(r.elapsedtime/(t.elapsedtime )) > 1.0		
			then concat('Fail: ',round(avg(r.elapsedtime/(t.elapsedtime)),2),' vs 2x Tgt') 
	      end  "Overall vs Price/Perf 2x Target" */
   from  run_hist  r, run_hist_target t
  where  r.starttime > now()-interval 3 day  
    and  r.rundesc = @rundesc
    and  r.description = 'ACB_4_tpch_1t'
    and  r.stmtdesc <> 'no-op' 
    and  r.description = t.description
    and  r.stmtdesc = t.stmtdesc
    and  r.sequence = t.sequence
    and  r.pm_count = t.pm_count;


-- -------------------------------------------------------------
-- Report on scaling elapsed
-- -------------------------------------------------------------
select 	 
--	 r.description, 
	 r.stmtdesc Qry,  r.sequence Run,
--	 substr(r.sqltext,1,20),  
	 round(avg(case when r.pm_count =1 then r.elapsedtime end),2) 1PM_Ela,  
	case when round(avg(case when r.pm_count =2 then r.elapsedtime end)/avg(case when r.pm_count =1 then r.elapsedtime end),2) 
		 <= .4 then	 concat('gtl: ',
		  round(avg(case when r.pm_count =2 then r.elapsedtime end)/avg(case when r.pm_count =1 then r.elapsedtime end),2),
		'x')
		when round(avg(case when r.pm_count =2 then r.elapsedtime end)/avg(case when r.pm_count =1 then r.elapsedtime end),2) 
		 <= .6 then	 concat('     ',
		  round(avg(case when r.pm_count =2 then r.elapsedtime end)/avg(case when r.pm_count =1 then r.elapsedtime end),2),
		'x')
		when round(avg(case when r.pm_count =2 then r.elapsedtime end)/avg(case when r.pm_count =1 then r.elapsedtime end),2) 
		> .6 then  	 concat('     ',
		  round(avg(case when r.pm_count =2 then r.elapsedtime end)/avg(case when r.pm_count =1 then r.elapsedtime end),2),
		'x Fail') 
	end "SF 1-->2 (tgt .50)",
	round(avg(case when r.pm_count =2 then r.elapsedtime end),2) 2PM_Ela,  
	case when round(avg(case when r.pm_count =4 then r.elapsedtime end)/avg(case when r.pm_count =2 then r.elapsedtime end),2) 
		 <= .4 then	 concat('gtl: ',
		  round(avg(case when r.pm_count =4 then r.elapsedtime end)/avg(case when r.pm_count =2 then r.elapsedtime end),2),
		'x')
		when round(avg(case when r.pm_count =4 then r.elapsedtime end)/avg(case when r.pm_count =2 then r.elapsedtime end),2) 
		 <= .6 then	 concat('     ',
		  round(avg(case when r.pm_count =4 then r.elapsedtime end)/avg(case when r.pm_count =2 then r.elapsedtime end),2),
		'x')
		when round(avg(case when r.pm_count =4 then r.elapsedtime end)/avg(case when r.pm_count =2 then r.elapsedtime end),2) 
		> .6 then 	 concat('     ',
		  round(avg(case when r.pm_count =4 then r.elapsedtime end)/avg(case when r.pm_count =2 then r.elapsedtime end),2),
		'x Fail') 
	end "SF 2-->4 (tgt .50)",
 	 round(avg(case when r.pm_count =4 then r.elapsedtime end),2) 4PM_Ela
/*	,
	case when round(avg(case when r.pm_count =4 then r.elapsedtime end)/avg(case when r.pm_count =1 then r.elapsedtime end),2) 
		 <= .2 then 	concat('gtl: ',
	  round(avg(case when r.pm_count =4 then r.elapsedtime end)/avg(case when r.pm_count =1 then r.elapsedtime end),2),
		'x')
		when round(avg(case when r.pm_count =4 then r.elapsedtime end)/avg(case when r.pm_count =1 then r.elapsedtime end),2) 
		 <= .3 then	 concat('     ',
	  round(avg(case when r.pm_count =4 then r.elapsedtime end)/avg(case when r.pm_count =1 then r.elapsedtime end),2),
		'x')
		when round(avg(case when r.pm_count =4 then r.elapsedtime end)/avg(case when r.pm_count =1 then r.elapsedtime end),2) 
		> .3 then 	concat('     ',
	  round(avg(case when r.pm_count =4 then r.elapsedtime end)/avg(case when r.pm_count =1 then r.elapsedtime end),2),
		'x Fail') 
	end "SF 1-->4 (tgt .25)" */
   from  run_hist  r, run_hist_target t
  where  r.starttime > now()-interval 3 day  
    and  r.rundesc = @rundesc
    and  r.description = 'ACB_4_tpch_1t'
    and  r.stmtdesc <> 'no-op' 
    and  r.description = t.description
    and  r.stmtdesc = t.stmtdesc
    and  r.sequence = t.sequence
    and  r.pm_count = t.pm_count
group by r.description, r.stmtdesc, r.sqltext, r.sequence   
order by r.description, r.stmtdesc,r.sequence;
