INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (rgprint_SRCS rgprint.cpp)

ADD_EXECUTABLE (rgprint ${rgprint_SRCS})

TARGET_LINK_LIBRARIES (rgprint ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_WRITE_LIBS})

INSTALL (
  TARGETS rgprint
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
