
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(ExeMgr_SRCS main.cpp activestatementcounter.cpp femsghandler.cpp ../utils/common/crashtrace.cpp)

add_executable(ExeMgr ${ExeMgr_SRCS})

target_link_libraries(ExeMgr ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} cacheutils threadpool)

target_include_directories(ExeMgr PRIVATE ${Boost_INCLUDE_DIRS})

install(TARGETS ExeMgr DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)


########### install files ###############


