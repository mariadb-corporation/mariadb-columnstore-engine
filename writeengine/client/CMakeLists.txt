INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (writeengineclient_LIB_SRCS we_clients.cpp we_ddlcommandclient.cpp we_dmlcommandclient.cpp)

ADD_LIBRARY (writeengineclient SHARED ${writeengineclient_LIB_SRCS})

ADD_DEPENDENCIES (writeengineclient loggingcpp)

TARGET_LINK_LIBRARIES (writeengineclient ${NETSNMP_LIBRARIES})

INSTALL (
  TARGETS writeengineclient
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
