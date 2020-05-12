
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(viewtablelock_SRCS viewtablelock.cpp)

add_executable(viewtablelock ${viewtablelock_SRCS})

target_link_libraries(viewtablelock ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS viewtablelock DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

