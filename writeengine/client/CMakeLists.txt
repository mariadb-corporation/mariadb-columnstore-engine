
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(writeengineclient_LIB_SRCS we_clients.cpp we_ddlcommandclient.cpp we_dmlcommandclient.cpp)

add_library(writeengineclient SHARED ${writeengineclient_LIB_SRCS})

add_dependencies(writeengineclient loggingcpp)

target_link_libraries(writeengineclient ${NETSNMP_LIBRARIES})

install(TARGETS writeengineclient DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)


