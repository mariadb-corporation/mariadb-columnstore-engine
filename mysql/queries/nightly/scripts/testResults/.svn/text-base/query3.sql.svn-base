/* Report time and memory by test. */
select tr.test, timediff(tr.stop, tr.start) run_time,  max(m.PrimProc) maxPP, max(m.ExeMgr) maxEM, max(m.DMLProc) maxDMP, max(m.cpimport) maxImp, max(controllernode) maxCntlr, max(workernode) maxWrkr, max(PrimProc + ExeMgr + DMLProc + DDLProc + ifnull(cpimport,0) + ifnull(workernode,0) + ifnull(controllernode,0)) maxAll
from testRun tr join testRunMem m
on tr.runId = m.runId and
   tr.test = m.test
where tr.runId = (select max(runId) from run)
group by 1, 2
order by 1;

