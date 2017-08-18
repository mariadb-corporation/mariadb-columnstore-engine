
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(postConfigure_SRCS postConfigure.cpp helpers.cpp)

add_executable(postConfigure ${postConfigure_SRCS})

target_link_libraries(postConfigure ${ENGINE_LDFLAGS} readline ncurses ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS postConfigure DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


########### next target ###############

set(installer_SRCS installer.cpp helpers.cpp)

add_executable(installer ${installer_SRCS})

target_link_libraries(installer ${ENGINE_LDFLAGS} readline ncurses ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS installer DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


########### next target ###############

set(getMySQLpw_SRCS getMySQLpw.cpp)

add_executable(getMySQLpw ${getMySQLpw_SRCS})

target_link_libraries(getMySQLpw ${ENGINE_LDFLAGS} readline ncurses ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS getMySQLpw DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


########### next target ###############

set(amazonInstaller_SRCS amazonInstaller.cpp helpers.cpp)

add_executable(amazonInstaller ${amazonInstaller_SRCS})

target_link_libraries(amazonInstaller ${ENGINE_LDFLAGS} readline ncurses ${SNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS amazonInstaller DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


########### next target ###############

set(mycnfUpgrade_SRCS mycnfUpgrade.cpp)

add_executable(mycnfUpgrade ${mycnfUpgrade_SRCS})

target_link_libraries(mycnfUpgrade ${ENGINE_LDFLAGS} readline ncurses ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS mycnfUpgrade DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

