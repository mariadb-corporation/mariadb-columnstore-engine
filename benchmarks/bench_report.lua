function sysbench.report_json(stat)
   if not gobj then
      io.write('[\n')
      -- hack to print the closing bracket when the Lua state of the reporting
      -- thread is closed
      gobj = newproxy(true)
      getmetatable(gobj).__gc = function () io.write('\n]\n') end
   else
      io.write(',\n')
   end

   local seconds = stat.time_interval
   io.write(([[
  {
    "time": %4.0f,
    "threads": %u,
    "transactions": %u,
    "queries": {
       "total": %u,
       "reads": %u,
       "writes": %u,
       "other": %u
    },
    "tps": %4.2f,
    "qps": {
      "total": %4.2f,
      "reads": %4.2f,
      "writes": %4.2f,
      "other": %4.2f
    },
    "latency": %4.2f,
    "errors": %4.2f,
    "reconnects": %4.2f
  }]]):format(
            stat.time_total,
            stat.threads_running,
            stat.events,
            stat.reads + stat.writes + stat.other,
            stat.reads, stat.writes, stat.other,
            stat.events / seconds,
            (stat.reads + stat.writes + stat.other) / seconds,
            stat.reads / seconds,
            stat.writes / seconds,
            stat.other / seconds,
            stat.latency_pct * 1000,
            stat.errors / seconds,
            stat.reconnects / seconds
   ))
end
