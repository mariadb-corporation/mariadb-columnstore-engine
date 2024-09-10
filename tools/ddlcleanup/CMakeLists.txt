INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (ddlcleanup_SRCS ddlcleanup.cpp)

ADD_EXECUTABLE (ddlcleanup ${ddlcleanup_SRCS})

TARGET_LINK_LIBRARIES (ddlcleanup ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_WRITE_LIBS} ddlcleanuputil)

INSTALL (
  TARGETS ddlcleanup
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
