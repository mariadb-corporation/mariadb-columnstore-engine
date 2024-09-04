INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (viewtablelock_SRCS viewtablelock.cpp)

ADD_EXECUTABLE (viewtablelock ${viewtablelock_SRCS})

TARGET_LINK_LIBRARIES (viewtablelock ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

INSTALL (
  TARGETS viewtablelock
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
