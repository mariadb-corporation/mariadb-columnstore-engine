
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(cspasswd_SRCS cspasswd.cpp secrets.cpp)
set(cskeys_SRCS cskeys.cpp secrets.cpp)

add_executable(cspasswd ${cspasswd_SRCS})
add_executable(cskeys ${cskeys_SRCS})

target_link_libraries(cspasswd ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})
target_link_libraries(cskeys ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS cspasswd DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
install(TARGETS cskeys DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

