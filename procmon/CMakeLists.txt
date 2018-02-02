
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(ProcMon_SRCS main.cpp processmonitor.cpp ../utils/common/crashtrace.cpp)

add_executable(ProcMon ${ProcMon_SRCS})

target_link_libraries(ProcMon ${ENGINE_LDFLAGS} cacheutils ${NETSNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS ProcMon DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

