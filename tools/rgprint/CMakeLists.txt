
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(rgprint_SRCS rgprint.cpp)

add_executable(rgprint ${rgprint_SRCS})

target_link_libraries(rgprint ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS})

install(TARGETS rgprint DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

