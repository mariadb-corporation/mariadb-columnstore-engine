require("bench_report")

function prepare ()
  local i
  print("creating table test.t1 ...")
  db_query("create table t1 (c1 int)engine=columnstore")
end

function cleanup()
  db_query("drop table t1")
end

function help()
  print("sysbench Lua demo; no special command line options available")
end

function thread_init(thread_id)
end

function thread_done(thread_id)
  db_disconnect()
end

function event(thread_id)
  db_query("select c1 from t1 where c1 = 5 or c1 = 10")
end

sysbench.hooks.report_intermediate = sysbench.report_json
sysbench.hooks.report_cumulative = sysbench.report_json
