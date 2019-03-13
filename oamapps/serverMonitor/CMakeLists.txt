
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(ServerMonitor_SRCS
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

add_executable(ServerMonitor ${ServerMonitor_SRCS})

target_link_libraries(ServerMonitor ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS ServerMonitor DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

