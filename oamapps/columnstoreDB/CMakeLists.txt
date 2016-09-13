
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(columnstoreDBWrite_SRCS columnstoreDB.cpp)

add_executable(columnstoreDBWrite ${columnstoreDBWrite_SRCS})

target_link_libraries(columnstoreDBWrite ${ENGINE_LDFLAGS} readline ncurses ${NETSNMP_LIBRARIES} ${ENGINE_EXEC_LIBS})

install(TARGETS columnstoreDBWrite DESTINATION ${ENGINE_BINDIR})

