INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (
  WriteEngineServer_SRCS
  we_server.cpp
  we_readthread.cpp
  we_ddlcommandproc.cpp
  we_observer.cpp
  we_dataloader.cpp
  we_brmrprtparser.cpp
  we_dmlcommandproc.cpp
  we_cleartablelockcmd.cpp
  we_cpifeederthread.cpp
  we_getfilesizes.cpp
  ../../utils/common/crashtrace.cpp
  ../../datatypes/mcs_datatype.cpp
)

ADD_EXECUTABLE (WriteEngineServer ${WriteEngineServer_SRCS})

ADD_DEPENDENCIES (WriteEngineServer loggingcpp)

TARGET_LINK_LIBRARIES (
  WriteEngineServer ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_WRITE_LIBS} threadpool writeengineredistribute
)

INSTALL (
  TARGETS WriteEngineServer
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
