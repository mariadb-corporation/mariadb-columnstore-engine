INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (columnstoreDBWrite_SRCS columnstoreDB.cpp)

ADD_EXECUTABLE (columnstoreDBWrite ${columnstoreDBWrite_SRCS})

TARGET_LINK_LIBRARIES (columnstoreDBWrite ${ENGINE_LDFLAGS} ncurses ${ENGINE_EXEC_LIBS})

INSTALL (
  TARGETS columnstoreDBWrite
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
