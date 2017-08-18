
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(mcsadmin_SRCS mcsadmin.cpp)

add_executable(mcsadmin ${mcsadmin_SRCS})

target_link_libraries(mcsadmin ${ENGINE_LDFLAGS} readline ncurses ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS} ${ENGINE_WRITE_LIBS})

install(TARGETS mcsadmin DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

