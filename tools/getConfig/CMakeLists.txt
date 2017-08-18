
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(getConfig_SRCS main.cpp)

add_executable(getConfig ${getConfig_SRCS})

target_link_libraries(getConfig ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS getConfig DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

