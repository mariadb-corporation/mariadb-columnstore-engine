INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (setConfig_SRCS main.cpp)

ADD_EXECUTABLE (mcsSetConfig ${setConfig_SRCS})

TARGET_LINK_LIBRARIES (mcsSetConfig ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_EXEC_LIBS})

INSTALL (
  TARGETS mcsSetConfig
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
