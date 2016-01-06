/* Report the total run time and mem usage for the baseline run and the latest run. */

select max(runId) into @runId from run;
select testfolder into @testfolder from run where runid=@runid;
select max(runId) into @baseRunId from testBaseline where testfolder=@testfolder;

/* Report total time and max memory. */
select if(r.runId = @runId, 'Current', 'Baseline') as Run, 
	r.start Start, 
	r.runId 'Run ID', 
	r.version Version, r.rel Rel, 
	timediff(r.stop, r.start) 'Run Time', 
	max(trm.ExeMgr) 'Max ExeMgr', 
	avg(trm.ExeMgr) 'Avg ExeMgr',
	count(distinct tr.test) 'Tests Run'
from run r 
left join testRun tr
on r.runId = tr.runId
left join testRunMem trm
on tr.runId = trm.runId and
   tr.test = trm.test
where r.runId in (@runId, @baseRunId)
group by 1, 2, 3, 4, 5;

