
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(setConfig_SRCS main.cpp)

add_executable(setConfig ${setConfig_SRCS})

add_dependencies(setConfig libnetsnmpmibs)

target_link_libraries(setConfig ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

install(TARGETS setConfig DESTINATION ${ENGINE_BINDIR})

install(PROGRAMS configxml.sh DESTINATION ${ENGINE_BINDIR})

