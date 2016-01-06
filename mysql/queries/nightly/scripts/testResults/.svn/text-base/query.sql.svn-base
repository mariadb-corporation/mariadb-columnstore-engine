/* Report total time and max memory. */
select r.runId, r.version, r.rel, r.buildDtm, timediff(r.stop, r.start) run_time, max(m.PrimProc) maxPP, max(m.ExeMgr) maxEM, max(m.DMLProc) maxDMP, max(m.cpimport) maxImp, max(controllernode) maxCntlr, max(PrimProc + ExeMgr + DMLProc + DDLProc + ifnull(cpimport,0) + ifnull(workernode,0) + ifnull(controllernode,0)) maxAll
from run r join testRunMem m
on r.runId = m.runId 
where r.runId = (select max(runId) from run)
group by 1, 2, 3, 4;

