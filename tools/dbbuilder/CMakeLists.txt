
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(dbbuilder_SRCS dbbuilder.cpp systemcatalog.cpp)

add_executable(dbbuilder ${dbbuilder_SRCS})

target_link_libraries(dbbuilder ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS})

install(TARGETS dbbuilder DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

