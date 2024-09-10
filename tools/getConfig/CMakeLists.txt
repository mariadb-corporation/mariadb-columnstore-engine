INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (getConfig_SRCS main.cpp)

ADD_EXECUTABLE (mcsGetConfig ${getConfig_SRCS})

TARGET_LINK_LIBRARIES (mcsGetConfig ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

INSTALL (
  TARGETS mcsGetConfig
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
