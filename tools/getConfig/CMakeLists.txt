
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(getConfig_SRCS main.cpp)

add_executable(mcsGetConfig ${getConfig_SRCS})

target_link_libraries(mcsGetConfig ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS mcsGetConfig DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

