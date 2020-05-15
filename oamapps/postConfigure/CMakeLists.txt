
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(postConfigure_SRCS postConfigure.cpp helpers.cpp)

add_executable(postConfigure ${postConfigure_SRCS})

target_compile_options(postConfigure PRIVATE -Wno-unused-result)

target_link_libraries(postConfigure ${ENGINE_LDFLAGS} ${ENGINE_READLINE_LIBRARY} ncurses ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS postConfigure DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)


########### next target ###############

set(installer_SRCS installer.cpp helpers.cpp)

add_executable(columnstore_installer ${installer_SRCS})

target_compile_options(columnstore_installer PRIVATE -Wno-unused-result)

target_link_libraries(columnstore_installer ${ENGINE_LDFLAGS} ${ENGINE_READLINE_LIBRARY} ncurses ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS columnstore_installer DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)


########### next target ###############

set(getMySQLpw_SRCS getMySQLpw.cpp)

add_executable(getMySQLpw ${getMySQLpw_SRCS})

target_link_libraries(getMySQLpw ${ENGINE_LDFLAGS} ${ENGINE_READLINE_LIBRARY} ncurses ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS getMySQLpw DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)


########### next target ###############

#set(amazonInstaller_SRCS amazonInstaller.cpp helpers.cpp)

#add_executable(amazonInstaller ${amazonInstaller_SRCS})

#target_link_libraries(amazonInstaller ${ENGINE_LDFLAGS} ${ENGINE_READLINE_LIBRARY} ncurses ${SNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

#install(TARGETS amazonInstaller DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-platform)


########### next target ###############

set(mycnfUpgrade_SRCS mycnfUpgrade.cpp)

add_executable(mycnfUpgrade ${mycnfUpgrade_SRCS})

target_compile_options(mycnfUpgrade PRIVATE -Wno-unused-result)

target_link_libraries(mycnfUpgrade ${ENGINE_LDFLAGS} ${ENGINE_READLINE_LIBRARY} ncurses ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS mycnfUpgrade DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)


########### next target ###############

install(PROGRAMS quick_installer_single_server.sh quick_installer_multi_server.sh 
        DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)               
