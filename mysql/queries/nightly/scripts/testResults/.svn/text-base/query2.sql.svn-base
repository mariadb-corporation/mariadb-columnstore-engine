/*
	Report latest timings by test versus the baseline test.
*/

select max(runId) into @runId from run;
select testfolder into @testfolder from run where runid=@runid;
select max(runId) into @baseRunId from testBaseline where testfolder=@testfolder;

select 	tr1.test Test, 
	timediff(tr1.stop, tr1.start) as 'Base Time', 
	timediff(tr2.stop, tr2.start) as 'Time',
	(substr(timediff(tr2.stop, tr2.start), 1, 2) * 3600 +
		substr(timediff(tr2.stop, tr2.start), 4, 2) * 60 + 
		substr(timediff(tr2.stop, tr2.start), 7, 2)) -
		(substr(timediff(tr1.stop, tr1.start), 1, 2) * 3600 +
		substr(timediff(tr1.stop, tr1.start), 4, 2) * 60 + 
		substr(timediff(tr1.stop, tr1.start), 7, 2)) diff,
	round(((substr(timediff(tr2.stop, tr2.start), 1, 2) * 3600 +
		substr(timediff(tr2.stop, tr2.start), 4, 2) * 60 + 
		substr(timediff(tr2.stop, tr2.start), 7, 2)) -
		(substr(timediff(tr1.stop, tr1.start), 1, 2) * 3600 +
		substr(timediff(tr1.stop, tr1.start), 4, 2) * 60 + 
		substr(timediff(tr1.stop, tr1.start), 7, 2))) / 
		(substr(timediff(tr1.stop, tr1.start), 1, 2) * 3600 +
		substr(timediff(tr1.stop, tr1.start), 4, 2) * 60 + 
		substr(timediff(tr1.stop, tr1.start), 7, 2)) * 100, 2) '% Diff',
	if(((substr(timediff(tr2.stop, tr2.start), 1, 2) * 3600 +
		substr(timediff(tr2.stop, tr2.start), 4, 2) * 60 + 
		substr(timediff(tr2.stop, tr2.start), 7, 2)) -
		(substr(timediff(tr1.stop, tr1.start), 1, 2) * 3600 +
		substr(timediff(tr1.stop, tr1.start), 4, 2) * 60 + 
		substr(timediff(tr1.stop, tr1.start), 7, 2))) / 
		(substr(timediff(tr1.stop, tr1.start), 1, 2) * 3600 +
		substr(timediff(tr1.stop, tr1.start), 4, 2) * 60 + 
		substr(timediff(tr1.stop, tr1.start), 7, 2)) > .10 
		&&	
	        (substr(timediff(tr2.stop, tr2.start), 1, 2) * 3600 +
                substr(timediff(tr2.stop, tr2.start), 4, 2) * 60 +
                substr(timediff(tr2.stop, tr2.start), 7, 2)) -
                (substr(timediff(tr1.stop, tr1.start), 1, 2) * 3600 +
                substr(timediff(tr1.stop, tr1.start), 4, 2) * 60 +
                substr(timediff(tr1.stop, tr1.start), 7, 2)) > 10
, 'Failed', 'Passed') 'Diff <= 10% or Diff <= 10'
from testrun tr1 
left join testrun tr2 
on tr1.test = tr2.test and tr2.runid = @runId
where tr1.runid = @baseRunId;
