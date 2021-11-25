
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(autoConfigure_SRCS autoConfigure.cpp)

add_executable(autoConfigure ${autoConfigure_SRCS})

target_link_libraries(autoConfigure ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

#install(TARGETS autoConfigure DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
