
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(mcsadmin_SRCS mcsadmin.cpp)

add_executable(mcsadmin ${mcsadmin_SRCS})

target_compile_options(mcsadmin PRIVATE -Wno-unused-result)

target_link_libraries(mcsadmin ${ENGINE_LDFLAGS} ${ENGINE_READLINE_LIBRARY} ncurses ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS} ${ENGINE_WRITE_LIBS})

install(TARGETS mcsadmin DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

