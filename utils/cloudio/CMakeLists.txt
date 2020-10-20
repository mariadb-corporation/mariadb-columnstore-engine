include_directories(${ENGINE_COMMON_INCLUDES} ${ENGINE_SRC_DIR}/storage-manager/include)

set(cloudio_LIB_SRCS SMComm.cpp SMDataFile.cpp SMFileFactory.cpp SMFileSystem.cpp SocketPool.cpp cloud_plugin.cpp ../../datatypes/mcs_datatype.cpp)

add_library(cloudio SHARED ${cloudio_LIB_SRCS})

# IDBDataFile currently depends on cloudio, which is backward.
# Once cloudio has been turned into a proper plugin for idbdatafile,
# we should be able to reverse the dependency like so:
target_link_libraries(cloudio idbdatafile messageqcpp loggingcpp)

install(TARGETS cloudio DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

add_executable(cloudio_component_test component_test.cpp)

add_executable(end_to_end_test end_to_end_test.cpp)

# see the comment above and change this dependency to cloudio.  Hm
# our lib dependencies seem not to be declared.  Punting on that,
# maybe in the future we can have some poor unfortunate intern
# untangle all of that and declare lib dependencies properly.
# For now I'm going to do like the other executables, which means
# nearly everything AFAICT.
target_link_libraries(cloudio_component_test ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS} cloudio)
target_link_libraries(end_to_end_test ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS} cloudio)
#target_link_libraries(cloudio_component_test cloudio)
# Copy end_to_end_test dataFile to binary directory
FILE(COPY testData DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/)

