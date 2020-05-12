
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(editem_SRCS editem.cpp)

add_executable(editem ${editem_SRCS})

target_link_libraries(editem ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS editem DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

