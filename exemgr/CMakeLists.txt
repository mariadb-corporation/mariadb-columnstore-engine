
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(ExeMgr_SRCS main.cpp activestatementcounter.cpp femsghandler.cpp)

add_executable(ExeMgr ${ExeMgr_SRCS})

target_link_libraries(ExeMgr ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES} cacheutils threadpool)

install(TARGETS ExeMgr DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


########### install files ###############


