include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(writeengineclient_LIB_SRCS we_clients.cpp we_ddlcommandclient.cpp we_dmlcommandclient.cpp)

columnstore_library(writeengineclient ${writeengineclient_LIB_SRCS})

add_dependencies(writeengineclient loggingcpp)

columnstore_link(writeengineclient ${NETSNMP_LIBRARIES})
