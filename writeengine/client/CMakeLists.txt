
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(writeengineclient_LIB_SRCS we_clients.cpp we_ddlcommandclient.cpp we_dmlcommandclient.cpp)

add_library(writeengineclient SHARED ${writeengineclient_LIB_SRCS})

target_link_libraries(writeengineclient ${NETSNMP_LIBRARIES})

set_target_properties(writeengineclient PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS writeengineclient DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


