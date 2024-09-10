INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (columnstoreSupport_SRCS columnstoreSupport.cpp mcsSupportUtil.cpp)

ADD_EXECUTABLE (columnstoreSupport ${columnstoreSupport_SRCS})

TARGET_COMPILE_OPTIONS (columnstoreSupport PRIVATE -Wno-unused-result)

TARGET_LINK_LIBRARIES (columnstoreSupport ${ENGINE_LDFLAGS} ncurses ${ENGINE_EXEC_LIBS})

INSTALL (
  TARGETS columnstoreSupport
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)

INSTALL (
  PROGRAMS dbmsReport.sh bulklogReport.sh configReport.sh hardwareReport.sh logReport.sh resourceReport.sh
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
