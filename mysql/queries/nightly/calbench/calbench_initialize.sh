#!/bin/sh
script_dir=$(dirname $0)
cd $script_dir

# capture software version				@sw_ver_var
/usr/local/Calpont/bin/calpontConsole getCalpontSoftwareInfo | grep Release | awk 'BEGIN{q="\047"}{print"Select "q$0q" colname into @sw_ver_var;"}' > ./global_tmp/load_calbench_metrics1.sql

# capture PM configuration 				@pm_count_var
/usr/local/Calpont/bin/calpontConsole getsystemstatus | grep "Module pm" | grep "ACTIVE"| grep -c -v "INACTIVE" | awk '{print"Select "$0" into @pm_count_var;"}' >> ./global_tmp/load_calbench_metrics1.sql

# capture UM configuration 				@um_count_var
/usr/local/Calpont/bin/calpontConsole getsystemstatus | grep "Module\ um\|Module\ dm" | grep "ACTIVE"| grep -c -v "INACTIVE" | awk '{print"Select "$0" into @um_count_var;"}' >> ./global_tmp/load_calbench_metrics1.sql

# capture a subset of configurable parameters:   
/usr/local/Calpont/bin/configxml.sh getconfig JobList MaxOutstandingRequests | awk 'BEGIN{q="\047"}{print"Select "q$8q" colname into @max_outstanding_var;"}' >> ./global_tmp/load_calbench_metrics1.sql
/usr/local/Calpont/bin/configxml.sh getconfig HashJoin PmMaxMemorySmallSide | awk 'BEGIN{q="\047"}{print"Select "q$8q" colname into @pm_max_var;"}' >> ./global_tmp/load_calbench_metrics1.sql
/usr/local/Calpont/bin/configxml.sh getconfig HashJoin UmMaxMemorySmallSide | awk 'BEGIN{q="\047"}{print"Select "q$8q" colname into @um_max_var;"}' >> ./global_tmp/load_calbench_metrics1.sql
/usr/local/Calpont/bin/configxml.sh getconfig  PrimitiveServers ProcessorThreshold | awk 'BEGIN{q="\047"}{print"Select "q$8q" colname into @proc_thresh_var;"}' >> ./global_tmp/load_calbench_metrics1.sql
/usr/local/Calpont/bin/configxml.sh getconfig  PrimitiveServers ConnectionsPerPrimProc | awk 'BEGIN{q="\047"}{print"Select "q$8q" colname into @conn_per_prim_var;"}' >> ./global_tmp/load_calbench_metrics1.sql
/usr/local/Calpont/bin/configxml.sh getconfig  PrimitiveServers RotatingDestination | awk 'BEGIN{q="\047"}{print"Select "q$8q" colname into @rotating_dest_var;"}' >> ./global_tmp/load_calbench_metrics1.sql
/usr/local/Calpont/bin/configxml.sh getconfig  DBBC NumThreads | awk 'BEGIN{q="\047"}{print"Select "q$8q" colname into @numthreads_var;"}' >> ./global_tmp/load_calbench_metrics1.sql
/usr/local/Calpont/bin/configxml.sh getconfig  DBBC NumBlocksPct | awk 'BEGIN{q="\047"}{print"Select "q$8q" colname into @numblockspct_var;"}' >> ./global_tmp/load_calbench_metrics1.sql

# Quick test of interconnect speed   
ethtool eth0 | grep Speed |  awk 'BEGIN{q="\047"}{print"Select "q$2 q" colname into @eth0_speed_var;"}' >>  ./global_tmp/load_calbench_metrics1.sql
ethtool eth0 | grep Speed |  awk 'BEGIN{q="\047"}{print"Select "q$2 q" colname into @eth1_speed_var;"}' >>  ./global_tmp/load_calbench_metrics1.sql



