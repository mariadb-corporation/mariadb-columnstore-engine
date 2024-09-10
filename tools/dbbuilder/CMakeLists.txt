INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (dbbuilder_SRCS dbbuilder.cpp systemcatalog.cpp)

ADD_EXECUTABLE (dbbuilder ${dbbuilder_SRCS})

TARGET_LINK_LIBRARIES (dbbuilder ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_WRITE_LIBS})

INSTALL (
  TARGETS dbbuilder
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
