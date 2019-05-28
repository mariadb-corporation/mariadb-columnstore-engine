
add_executable(ServerMonitor 
    main.cpp
    serverMonitor.cpp
    cpuMonitor.cpp
    diskMonitor.cpp
    memoryMonitor.cpp
    procmonMonitor.cpp
    msgProcessor.cpp
    dbhealthMonitor.cpp
    UMAutoSync.cpp
    ../../utils/common/crashtrace.cpp)

target_link_libraries(ServerMonitor 
  PRIVATE alarmmanager_h
          brm_h
          common_h
          configcpp_h
          idbdatafile_h 
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h )

target_pch(ServerMonitor)

install(TARGETS ServerMonitor DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

