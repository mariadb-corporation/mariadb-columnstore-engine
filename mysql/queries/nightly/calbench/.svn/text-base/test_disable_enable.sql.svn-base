\! /usr/local/Calpont/bin/calpontConsole getsystemstatus | grep "Module pm" | grep ' ACTIVE ' | wc -l | awk '{print "-- -- --  " $0" Active PMs  -- -- --"}'

\. flush
-- elapsed time with 0% cache hit (all PIO)
select count(*) from lineorder;
-- elapsed time with 100% cache hit (all LIO)
select count(*) from lineorder;

\! sleep 40 | /usr/local/Calpont/bin/calpontConsole altersystem-disablemodule pm3,pm4 y | grep Modules
\! /usr/local/Calpont/bin/calpontConsole getsystemstatus | grep "Module pm" | grep ' ACTIVE ' | wc -l | awk '{print "-- -- --  " $0" Active PMs  -- -- --"}'

-- elapsed time with variable cache hit %
select count(*) from lineorder;
\. flush
-- elapsed time with 0% cache hit (all PIO)
select count(*) from lineorder;
-- elapsed time with 100% cache hit (all LIO)
select count(*) from lineorder;

\! /usr/local/Calpont/bin/calpontConsole altersystem-enablemodule pm3,pm4 y | grep Modules
\! /usr/local/Calpont/bin/calpontConsole getsystemstatus | grep "Module pm" | grep ' ACTIVE ' | wc -l | awk '{print "-- -- --  " $0" Active PMs  -- -- --"}'

-- elapsed time with variable cache hit %
select count(*) from lineorder;
\. flush
-- elapsed time with 0% cache hit (all PIO)
select count(*) from lineorder;
-- elapsed time with 100% cache hit (all LIO)
select count(*) from lineorder;

