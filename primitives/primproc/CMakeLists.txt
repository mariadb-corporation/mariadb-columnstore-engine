INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES} ../blockcache ../linux-port)

# ########## next target ###############

SET (
  PrimProc_SRCS
  primproc.cpp
  batchprimitiveprocessor.cpp
  bppseeder.cpp
  bppsendthread.cpp
  columncommand.cpp
  command.cpp
  dictstep.cpp
  filtercommand.cpp
  logger.cpp
  passthrucommand.cpp
  primitiveserver.cpp
  pseudocc.cpp
  rtscommand.cpp
  umsocketselector.cpp
  serviceexemgr.cpp
  sqlfrontsessionthread.cpp
  rssmonfcn.cpp
  activestatementcounter.cpp
  femsghandler.cpp
  ../../utils/common/crashtrace.cpp
)

ADD_EXECUTABLE (PrimProc ${PrimProc_SRCS})

ADD_DEPENDENCIES (PrimProc loggingcpp)
TARGET_INCLUDE_DIRECTORIES (PrimProc PRIVATE ${Boost_INCLUDE_DIRS})
TARGET_LINK_LIBRARIES (
  PrimProc
  ${ENGINE_LDFLAGS}
  ${NETSNMP_LIBRARIES}
  ${ENGINE_WRITE_LIBS}
  threadpool
  cacheutils
  dbbc
  processor
)

INSTALL (
  TARGETS PrimProc
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
