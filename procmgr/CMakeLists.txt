
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(ProcMgr_SRCS main.cpp processmanager.cpp)

add_executable(ProcMgr ${ProcMgr_SRCS})

target_link_libraries(ProcMgr ${ENGINE_LDFLAGS} cacheutils ${ENGINE_EXEC_LIBS})

install(TARGETS ProcMgr DESTINATION ${ENGINE_BINDIR})

