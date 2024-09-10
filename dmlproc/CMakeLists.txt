INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (DMLProc_SRCS dmlproc.cpp dmlprocessor.cpp dmlresultbuffer.cpp batchinsertprocessor.cpp
                  ../utils/common/crashtrace.cpp
)

ADD_EXECUTABLE (DMLProc ${DMLProc_SRCS})

ADD_DEPENDENCIES (DMLProc loggingcpp)

TARGET_LINK_LIBRARIES (
  DMLProc
  ${ENGINE_LDFLAGS}
  ${ENGINE_WRITE_LIBS}
  ${NETSNMP_LIBRARIES}
  threadpool
  ddlcleanuputil
  batchloader
)

INSTALL (
  TARGETS DMLProc
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
