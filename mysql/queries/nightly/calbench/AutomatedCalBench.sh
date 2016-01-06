#!/bin/sh
script_dir=$(dirname $0)
cd $script_dir

echo "AutomatedCalBenchStarted"

/usr/local/Calpont/bin/calpontConsole getsysteminfo >> systeminfo.log


./remote_command.sh srvalpha3 qalpont! /home/calbench/setparms.sh

/usr/local/Calpont/bin/calpontConsole getsysteminfo >> systeminfo.log

/usr/local/Calpont/bin/calpontConsole restartsystem y
sleep 15

/usr/local/Calpont/bin/calpontConsole getsysteminfo >> systeminfo.log

/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf test -e 'insert into run_hist (description,starttime) values (
	concat("testing-Start:  ", date_format(now(),"%c-%e %T") )	,now() );'

echo '--------------------------------------------------------------------'
echo '-- Scan Rate, Concurrency Tests'
echo '--------------------------------------------------------------------'

# PENDING ENHANCEMENT 2693
# \. calbench_nohup_n_times.sh 1 scan_scalability_1000.sql 26 1200
# \. calbench_nohup_n_times.sh 1 scan_repeat_1000.sql 15 1500
# \. calbench_nohup_n_times.sh 1 scan_concurrency_100.sql 20 5000
# \. calbench_nohup_n_times.sh 2 scan_concurrency_100.sql 20 5000
# \. calbench_nohup_n_times.sh 4 scan_concurrency_100.sql 20 5000
# \. calbench_nohup_n_times.sh 8 scan_concurrency_100.sql 20 5000
# \. calbench_nohup_n_times.sh 16 scan_concurrency_100.sql 20 5000
# \. calbench_nohup_n_times.sh 32 scan_concurrency_100.sql 20 5000

# \. calbench_nohup_n_times.sh 1 key_lookup_concurrency.sql 200 5000
# \. calbench_nohup_n_times.sh 2 key_lookup_concurrency.sql 200 5000
# \. calbench_nohup_n_times.sh 4 key_lookup_concurrency.sql 200 5000
# \. calbench_nohup_n_times.sh 8 key_lookup_concurrency.sql 200 5000
# \. calbench_nohup_n_times.sh 16 key_lookup_concurrency.sql 200 5000
# \. calbench_nohup_n_times.sh 32 key_lookup_concurrency.sql 200 5000

echo '--------------------------------------------------------------------'
echo '-- SSB, TPCH, PIO Scalabilty @ 4 PM'
echo '--------------------------------------------------------------------'

\. calbench_nohup_n_times.sh 1 ACB_ssb_100.sql 26 5000
\. calbench_nohup_n_times.sh 1 ACB_ssb_1000.sql 19 10000

\. calbench_nohup_n_times.sh 1 ACB_tpch100_1_6_12_14_3execs.sql 12 2400
# \. calbench_nohup_n_times.sh 1 ACB_tpch1t_1_6_12_14_3execs.sql 7 7200

\. calbench_nohup_n_times.sh 1 ssb_join_2_to_5_tables_100sf.sql 8 3000
\. calbench_nohup_n_times.sh 1 ssb_join_2_to_5_tables_1000sf.sql 8 3000

\. calbench_nohup_n_times.sh 1 pio_scalability.sql 13 1200
\. calbench_nohup_n_times.sh 1 bka_test.sql 5 200

\. calbench_nohup_n_times.sh 1 count_cust.sql 2 400
\. calbench_nohup_n_times.sh 1 count_part.sql 2 400
\. calbench_nohup_n_times.sh 1 count_partsupp.sql 2 600
\. calbench_nohup_n_times.sh 1 count_orders.sql 2 2000
\. calbench_nohup_n_times.sh 1 count_lineitem.sql 2 4000

\. calbench_nohup_n_times.sh 1 aggregate_1.sql 2 400
\. calbench_nohup_n_times.sh 1 aggregate_3.sql 2 800
\. calbench_nohup_n_times.sh 1 aggregate_5.sql 2 2400

echo '--------------------------------------------------------------------'
echo '-- Start Configure System ------------------------------------------'
echo '--------------------------------------------------------------------'
/usr/local/Calpont/bin/calpontConsole getsysteminfo >> systeminfo.log

time /usr/local/Calpont/bin/calpontConsole altersystem-disablemodule pm3,pm4 y
sleep 15
/usr/local/Calpont/bin/calpontConsole restartsystem y
sleep 15
/usr/local/Calpont/bin/calpontConsole getsysteminfo >> systeminfo.log


echo '--------------------------------------------------------------------'
echo '-- End Configure System --------------------------------------------'
echo '--------------------------------------------------------------------'


echo '--------------------------------------------------------------------'
echo '-- SSB, TPCH, PIO Scalabilty @ 2 PM '
echo '--------------------------------------------------------------------'
\. calbench_nohup_n_times.sh 1 ACB_ssb_100.sql 26 7000
\. calbench_nohup_n_times.sh 1 ACB_ssb_1000.sql 19 7000

\. calbench_nohup_n_times.sh 1 ACB_tpch100_1_6_12_14_3execs.sql 12 3600
# \. calbench_nohup_n_times.sh 1 ACB_tpch1t_6_12_14_3execs.sql 6 7200

\. calbench_nohup_n_times.sh 1 ssb_join_2_to_5_tables_100sf.sql 8 3000
\. calbench_nohup_n_times.sh 1 ssb_join_2_to_5_tables_1000sf.sql 8 3000

\. calbench_nohup_n_times.sh 1 pio_scalability.sql 13 5000
\. calbench_nohup_n_times.sh 1 bka_test.sql 5 2400


\. calbench_nohup_n_times.sh 1 count_cust.sql 2 400
\. calbench_nohup_n_times.sh 1 count_part.sql 2 400
\. calbench_nohup_n_times.sh 1 count_partsupp.sql 2 600
\. calbench_nohup_n_times.sh 1 count_orders.sql 2 2000
\. calbench_nohup_n_times.sh 1 count_lineitem.sql 2 4000

\. calbench_nohup_n_times.sh 1 aggregate_1.sql 2 400
\. calbench_nohup_n_times.sh 1 aggregate_3.sql 2 800
\. calbench_nohup_n_times.sh 1 aggregate_5.sql 2 2400



echo '--------------------------------------------------------------------'
echo '-- Start Configure System ------------------------------------------'
echo '--------------------------------------------------------------------'
/usr/local/Calpont/bin/calpontConsole getsysteminfo >> systeminfo.log

time /usr/local/Calpont/bin/calpontConsole altersystem-disablemodule pm2 y
sleep 15
/usr/local/Calpont/bin/calpontConsole restartsystem y
sleep 15

/usr/local/Calpont/bin/calpontConsole getsysteminfo >> systeminfo.log

echo '--------------------------------------------------------------------'
echo '-- End Configure System --------------------------------------------'
echo '--------------------------------------------------------------------'


echo '--------------------------------------------------------------------'
echo '-- SSB, TPCH, PIO Scalabilty @ 1 PM'
echo '--------------------------------------------------------------------'
\. calbench_nohup_n_times.sh 1 ACB_ssb_100.sql 26 12000

# about 1 hour invested here:  recommend not run routinely
# \. calbench_nohup_n_times.sh 1 ACB_ssb_1000.sql 19 12000

\. calbench_nohup_n_times.sh 1 pio_scalability.sql 13 5000
\. calbench_nohup_n_times.sh 1 bka_test.sql 5 2000

\. calbench_nohup_n_times.sh 1 ACB_tpch100_1_6_12_14_3execs.sql 12 4800

# about 24 minutes invested here:  recommend not run routinely
# \. calbench_nohup_n_times.sh 1 ACB_tpch1t_6_12_14_3execs.sql 6 12200


\. calbench_nohup_n_times.sh 1 ssb_join_2_to_5_tables_100sf.sql 8 6000
\. calbench_nohup_n_times.sh 1 ssb_join_2_to_5_tables_1000sf.sql 8 6000

\. calbench_nohup_n_times.sh 1 count_cust.sql 2 400
\. calbench_nohup_n_times.sh 1 count_part.sql 2 400
\. calbench_nohup_n_times.sh 1 count_partsupp.sql 2 600
\. calbench_nohup_n_times.sh 1 count_orders.sql 2 2000
\. calbench_nohup_n_times.sh 1 count_lineitem.sql 2 4000

\. calbench_nohup_n_times.sh 1 aggregate_1.sql 2 400
\. calbench_nohup_n_times.sh 1 aggregate_3.sql 2 800
\. calbench_nohup_n_times.sh 1 aggregate_5.sql 2 2400


echo '--------------------------------------------------------------------'
echo '-- Start Configure System ------------------------------------------'
echo '--------------------------------------------------------------------'
/usr/local/Calpont/bin/calpontConsole getsysteminfo >> systeminfo.log

time /usr/local/Calpont/bin/calpontConsole altersystem-enablemodule pm2,pm3,pm4 y
sleep 20
/usr/local/Calpont/bin/calpontConsole restartsystem y
sleep 20

/usr/local/Calpont/bin/calpontConsole getsysteminfo >> systeminfo.log

echo '--------------------------------------------------------------------'
echo '-- End Configure System --------------------------------------------'
echo '--------------------------------------------------------------------'


echo '--------------------------------------------------------------------'
echo '-- PIO Tests 1,2,4 concurrent access'
echo '--------------------------------------------------------------------'

\. calbench_nohup_n_times.sh 1 pio_concurrency_1.sql 4 3600

\. calbench_nohup_n_times.sh 1 pio_concurrency_2a.sql 0 5000
\. calbench_nohup_n_times_c2.sh 1 pio_concurrency_2b.sql 8 5000

\. calbench_nohup_n_times.sh 1 pio_concurrency_4a.sql 0 6000
\. calbench_nohup_n_times_c2.sh 1 pio_concurrency_4b.sql 0 6000
\. calbench_nohup_n_times_c3.sh 1 pio_concurrency_4c.sql 0 6000
\. calbench_nohup_n_times_c4.sh 1 pio_concurrency_4d.sql 16 6000


echo '--------------------------------------------------------------------'
echo '-- PIO Repeatability/Stability'
echo '--------------------------------------------------------------------'
\. calbench_nohup_n_times.sh 1 pio_repeat.sql 15 3000


echo '--------------------------------------------------------------------'
echo '-- Read Ahead Optimization'
echo '--------------------------------------------------------------------'
\. calbench_nohup_n_times.sh 1 read_ahead_test.sql 7 2500


echo '--------------------------------------------------------------------'
echo '-- Return Rate, 10 use cases'
echo '--------------------------------------------------------------------'
 \. calbench_nohup_n_times.sh 1 ACB_return_rate.sql 10 3000

echo '--------------------------------------------------------------------'
echo '-- Concurrent Return Rate for 1,2,4,8,16,32'
echo '--------------------------------------------------------------------'
 \. calbench_nohup_n_times.sh 1 ACB_return_rate_concurrent.sql 2 3000
 \. calbench_nohup_n_times.sh 2 ACB_return_rate_concurrent.sql 1 3000
 \. calbench_nohup_n_times.sh 4 ACB_return_rate_concurrent.sql 1 3000
 \. calbench_nohup_n_times.sh 8 ACB_return_rate_concurrent.sql 1 3000
 \. calbench_nohup_n_times.sh 16 ACB_return_rate_concurrent.sql 1 6000
 \. calbench_nohup_n_times.sh 32 ACB_return_rate_concurrent.sql 1 12000

echo '--------------------------------------------------------------------'
echo '-- Stability of Return Rate for 1,5,all columns'
echo '--------------------------------------------------------------------'
 \. calbench_nohup_n_times.sh 1 ACB_return_rate_stability_1.sql 20 3000
 \. calbench_nohup_n_times.sh 1 ACB_return_rate_stability_5.sql 10 3000
 \. calbench_nohup_n_times.sh 1 ACB_return_rate_stability_all.sql 5 3000



/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf test -e 'insert into run_hist (description,starttime) values (
	concat("testing-End:  ", date_format(now(),"%c-%e %T") )	,now() );'
sleep 10
/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root test -f < run_upd_rundesc.sql  

echo 'EndAutomatedCalBench'



