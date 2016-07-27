
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(mcsadmin_SRCS mcsadmin.cpp)

add_executable(mcsadmin ${mcsadmin_SRCS})

add_dependencies(mcsadmin libnetsnmpmibs)

target_link_libraries(mcsadmin ${ENGINE_LDFLAGS} readline ncurses ${ENGINE_EXEC_LIBS})

install(TARGETS mcsadmin DESTINATION ${ENGINE_BINDIR})

