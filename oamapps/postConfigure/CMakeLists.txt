
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(mycnfUpgrade_SRCS mycnfUpgrade.cpp)

add_executable(mycnfUpgrade ${mycnfUpgrade_SRCS})

target_compile_options(mycnfUpgrade PRIVATE -Wno-unused-result)

target_link_libraries(mycnfUpgrade ${ENGINE_LDFLAGS} ${ENGINE_READLINE_LIBRARY} ncurses ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS mycnfUpgrade DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
       
