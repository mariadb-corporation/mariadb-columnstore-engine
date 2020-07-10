
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(DDLProc_SRCS ddlproc.cpp ddlprocessor.cpp ../utils/common/crashtrace.cpp)

add_executable(DDLProc ${DDLProc_SRCS})

add_dependencies(DDLProc loggingcpp)

target_link_libraries(DDLProc ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} ${NETSNMP_LIBRARIES} threadpool)

install(TARGETS DDLProc DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

