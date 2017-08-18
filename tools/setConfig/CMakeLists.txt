
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(setConfig_SRCS main.cpp)

add_executable(setConfig ${setConfig_SRCS})

target_link_libraries(setConfig ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS setConfig DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

install(PROGRAMS configxml.sh DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

