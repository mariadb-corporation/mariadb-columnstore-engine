include_directories(${ENGINE_COMMON_INCLUDES})

find_library(FDB_LIB fdb_c)
if (NOT FDB_LIB)
    MESSAGE_ONCE(FATAL_ERROR "FoundationDB not installed properly, please install packages foundationdb-cliends and foundationdb-server")
    return()
endif()

add_library(fdbcs SHARED src/fdbcs.cpp)
target_link_libraries(fdbcs fdb_c)

add_executable(test_fdb_api test/test_fdb_api.cpp)
add_dependencies(test_fdb_api fdbcs)
target_link_libraries(test_fdb_api fdbcs fdb_c)

install(TARGETS fdbcs
  LIBRARY DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)

install(TARGETS test_fdb_api
    RUNTIME DESTINATION ${ENGINE_BINDIR}
    COMPONENT columnstore-engine
)
