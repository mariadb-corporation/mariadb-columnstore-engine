INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (cleartablelock_SRCS cleartablelock.cpp cleartablelockthread.cpp)

ADD_EXECUTABLE (cleartablelock ${cleartablelock_SRCS})

TARGET_LINK_LIBRARIES (cleartablelock ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_WRITE_LIBS})

INSTALL (
  TARGETS cleartablelock
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
