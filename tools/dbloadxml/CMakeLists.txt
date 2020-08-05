
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(dbload_STAT_SRCS inputmgr.cpp)

add_library(dbload STATIC ${dbload_STAT_SRCS})

add_dependencies(dbload loggingcpp)

########### next target ###############

set(colxml_SRCS colxml.cpp)

add_executable(colxml ${colxml_SRCS})

target_link_libraries(colxml ${ENGINE_LDFLAGS} dbload ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS})

install(TARGETS colxml DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

