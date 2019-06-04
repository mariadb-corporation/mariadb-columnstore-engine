
add_executable(ProcMgr main.cpp processmanager.cpp )

target_link_libraries(ProcMgr 
  PRIVATE alarmmanager
          brm
          cacheutils_h
          columnstore_h
          common                                                                            
          configcpp
          crashtrace
          ddlcleanuputil_h
          execplan_h
          funcexp_h
          idbboot_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp
          rwlock_h
          threadpool_h)

install(TARGETS ProcMgr DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

