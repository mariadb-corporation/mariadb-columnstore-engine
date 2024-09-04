INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

FIND_LIBRARY (FDB_LIB fdb_c)
IF (NOT FDB_LIB)
  MESSAGE_ONCE (
    FATAL_ERROR
    "FoundationDB not installed properly, please install packages foundationdb-cliends and foundationdb-server"
  )
  RETURN ()
ENDIF ()

ADD_LIBRARY (fdbcs SHARED src/fdbcs.cpp)
TARGET_LINK_LIBRARIES (fdbcs fdb_c)

ADD_EXECUTABLE (test_fdb_api test/test_fdb_api.cpp)
ADD_DEPENDENCIES (test_fdb_api fdbcs)
TARGET_LINK_LIBRARIES (test_fdb_api fdbcs fdb_c)

INSTALL (TARGETS fdbcs LIBRARY DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

INSTALL (TARGETS test_fdb_api RUNTIME DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
