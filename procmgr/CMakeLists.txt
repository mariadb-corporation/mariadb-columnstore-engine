
add_executable(ProcMgr main.cpp processmanager.cpp ../utils/common/crashtrace.cpp)

target_link_libraries(ProcMgr 
  PRIVATE alarmmanager_h
          brm_h
          cacheutils_h
          columnstore_h
          common_h
          configcpp_h
          ddlcleanuputil_h
          funcexp_h
          idbboot_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h
          threadpool_h)

install(TARGETS ProcMgr DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

