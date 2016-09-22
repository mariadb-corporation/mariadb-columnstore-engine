
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(ProcMon_SRCS main.cpp processmonitor.cpp)

add_executable(ProcMon ${ProcMon_SRCS})

add_dependencies(ProcMon libnetsnmpmibs)

target_link_libraries(ProcMon ${ENGINE_LDFLAGS} cacheutils ${ENGINE_EXEC_LIBS})

install(TARGETS ProcMon DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

