INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (dbload_STAT_SRCS inputmgr.cpp)

ADD_LIBRARY (dbload STATIC ${dbload_STAT_SRCS})

ADD_DEPENDENCIES (dbload loggingcpp)

# ########## next target ###############

SET (colxml_SRCS colxml.cpp)

ADD_EXECUTABLE (colxml ${colxml_SRCS})

TARGET_LINK_LIBRARIES (colxml ${ENGINE_LDFLAGS} dbload ${ENGINE_WRITE_LIBS})

INSTALL (
  TARGETS colxml
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
