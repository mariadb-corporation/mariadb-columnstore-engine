
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(autoInstaller_SRCS autoInstaller.cpp)

add_executable(autoInstaller ${autoInstaller_SRCS})

add_dependencies(autoInstaller libnetsnmpmibs)

target_link_libraries(autoInstaller ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS} readline ncurses)

install(TARGETS autoInstaller DESTINATION ${ENGINE_BINDIR})


########### next target ###############

set(autoConfigure_SRCS autoConfigure.cpp)

add_executable(autoConfigure ${autoConfigure_SRCS})

add_dependencies(autoConfigure libnetsnmpmibs)

target_link_libraries(autoConfigure ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

install(TARGETS autoConfigure DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


########### next target ###############

set(svnQuery_SRCS svnQuery.cpp)

add_executable(svnQuery ${svnQuery_SRCS})

add_dependencies(svnQuery libnetsnmpmibs)

target_link_libraries(svnQuery ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

install(TARGETS svnQuery DESTINATION ${ENGINE_BINDIR})

