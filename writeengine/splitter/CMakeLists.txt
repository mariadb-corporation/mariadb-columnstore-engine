INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES} ${S3API_DIR})

# ########## next target ###############

SET (
  cpimport_SRCS
  we_splitterapp.cpp
  we_cmdargs.cpp
  we_sdhandler.cpp
  we_respreadthread.cpp
  we_filereadthread.cpp
  we_splclient.cpp
  we_brmupdater.cpp
  we_tablelockgrabber.cpp
  we_xmlgetter.cpp
)

ADD_EXECUTABLE (cpimport ${cpimport_SRCS})

ADD_DEPENDENCIES (cpimport loggingcpp)

TARGET_LINK_LIBRARIES (
  cpimport
  ${ENGINE_LDFLAGS}
  ${NETSNMP_LIBRARIES}
  ${ENGINE_WRITE_LIBS}
  batchloader
  threadpool
  marias3
)

INSTALL (
  TARGETS cpimport
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
