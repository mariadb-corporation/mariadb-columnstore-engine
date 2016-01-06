# ---------------------------------------------------------
# usage:
# \. calbench_nohup_n_times.sh <concurrent executions of file> <file_to_be_run> <number of execution per session> <max seconds to sleep >
# \. calbench_nohup_n_times.sh 2 test.sql 1
# to run test.sql from 2 concurrent sessions when test.sql inserts one record into run_hist
# ---------------------------------------------------------
#!/bin/sh
script_dir=$(dirname $0)
cd $script_dir

\. calbench_initialize.sh
echo 'select '$1' into @concurrent_var;' >>  $script_dir/global_tmp/load_calbench_metrics1.sql
echo 'select '$3' into @execs_var;' >>  $script_dir/global_tmp/load_calbench_metrics1.sql


starting_test_count=`
/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf test << eof
select count(*) from test.run_hist;
eof` 

starting_test_count=`echo $starting_test_count | awk '{print $2 }'` 

echo 'call test.sp_sleep_until('$starting_test_count'+('$1'*'$3'),'$4');' > $script_dir/global_tmp/calbench_sleep_until.sql

echo "set @run_var := 1;" > $script_dir/global_tmp/calbench_start_execute_end.sql
echo "\. c_local.sql" >> $script_dir/global_tmp/calbench_start_execute_end.sql

for ((  i = 1 ;  i <= $3;  i++  ))
do
	echo "\. calbench_start.sql" >> $script_dir/global_tmp/calbench_start_execute_end.sql
	echo "EXECUTE stmt;" >> $script_dir/global_tmp/calbench_start_execute_end.sql
	echo "\. calbench_end.sql"  >> $script_dir/global_tmp/calbench_start_execute_end.sql
done 

for ((  i = 1 ;  i <= $1;  i++  ))
do
  "cp" $script_dir/$2 $script_dir/c$i/$2
  "cp" $script_dir/global_tmp/calbench_start_execute_end.sql $script_dir/c$i/calbench_start_execute_end.sql
  awk -v avar="$i" '{gsub("_dir",avar);print}'  $script_dir/calbench_c_local.sql > $script_dir/c$i/c_local.sql
  cd $script_dir/c$i/
  nohup  /usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root test < $script_dir/c$i/$2  > $script_dir/c$i/tmp_files/run.log 2> $script_dir/c$i/tmp_files/error.txt &
  sleep .01
done 

/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root test < $script_dir/global_tmp/calbench_sleep_until.sql  
 
cd $script_dir

# ls -ltrh ./c*/tmp_files/out.log | tail -$1
# ls -ltrh ./c*/tmp_files/error.txt | tail -$1
