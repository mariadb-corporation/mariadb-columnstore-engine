
add_executable(ProcMon main.cpp processmonitor.cpp ../utils/common/crashtrace.cpp)

target_link_libraries(ProcMon 
  PRIVATE alarmmanager_h
          brm_h
          cacheutils_h
          columnstore_h
          common_h
          compress_h
          configcpp_h
          ddlcleanuputil_h
          idbboot_h
          idbdatafile_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h
          rt)


install(TARGETS ProcMon DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

