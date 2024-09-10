INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (DDLProc_SRCS ddlproc.cpp ddlprocessor.cpp ../utils/common/crashtrace.cpp)

ADD_EXECUTABLE (DDLProc ${DDLProc_SRCS})

ADD_DEPENDENCIES (DDLProc loggingcpp)

TARGET_LINK_LIBRARIES (DDLProc ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${NETSNMP_LIBRARIES} threadpool)

INSTALL (
  TARGETS DDLProc
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
