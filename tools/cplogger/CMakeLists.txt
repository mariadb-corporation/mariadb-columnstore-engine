INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (cplogger_SRCS main.cpp)

ADD_EXECUTABLE (cplogger ${cplogger_SRCS})

TARGET_LINK_LIBRARIES (cplogger ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

INSTALL (
  TARGETS cplogger
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
