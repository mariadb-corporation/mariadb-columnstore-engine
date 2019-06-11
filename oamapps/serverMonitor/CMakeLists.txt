
cs_add_executable(ServerMonitor 
    main.cpp
    serverMonitor.cpp
    cpuMonitor.cpp
    diskMonitor.cpp
    memoryMonitor.cpp
    procmonMonitor.cpp
    msgProcessor.cpp
    dbhealthMonitor.cpp
    UMAutoSync.cpp
    )

target_link_libraries(ServerMonitor 
  PRIVATE alarmmanager_h
          joblist
          brm
          common_h
          crashtrace
          idbboot_h
          )


install(TARGETS ServerMonitor DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

