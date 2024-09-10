INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (editem_SRCS editem.cpp)

ADD_EXECUTABLE (editem ${editem_SRCS})

TARGET_LINK_LIBRARIES (editem ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_EXEC_LIBS})

INSTALL (
  TARGETS editem
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
