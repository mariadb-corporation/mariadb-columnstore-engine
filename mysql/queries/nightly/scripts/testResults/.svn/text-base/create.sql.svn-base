create table run (
runId int,
version varchar(10),
rel varchar(10),
start datetime,
stop datetime,
buildDtm varchar(30),
testFolder varchar(20)
);

create index idx_run on run (runId);

create table if not exists testRun (
runId int,
test char(7),
start datetime,
stop datetime,
status varchar(200));

create index idx_testRun on testRun (runId, test);

create table if not exists testRunMem (
runId int,
test varchar(20),
dtm datetime,
PrimProc decimal(4,1),
ExeMgr decimal(4,1),
DMLProc decimal(4,1),
cpimport decimal(4,1),
DDLProc decimal(4,1),
controllernode decimal(4,1),
workernode decimal(4,1));

create index idx_testRunMem on testRunMem (runId, test);

create table testBaseline (
runId int,
testfolder varchar(20)
);

