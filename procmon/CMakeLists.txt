
add_executable(ProcMon main.cpp processmonitor.cpp)

target_link_libraries(ProcMon 
  PRIVATE alarmmanager_h
          brm
          cacheutils_h
          columnstore_h
          common_h
          compress_h
          configcpp_h
          crashtrace
          ddlcleanuputil_h
          execplan_h
          idbboot
          idbdatafile
          joblist_h
          rt
          rwlock_h
          Boost::thread)


install(TARGETS ProcMon DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

