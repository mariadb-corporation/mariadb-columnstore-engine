include_directories(${ENGINE_COMMON_INCLUDES} ${CMAKE_SOURCE_DIR}/storage-manager/include)

set(cloudio_LIB_SRCS SMComm.cpp SMDataFile.cpp SMFileFactory.cpp SMFileSystem.cpp SocketPool.cpp cloud_plugin.cpp)

add_library(cloudio SHARED ${cloudio_LIB_SRCS})

# IDBDataFile currently depends on cloudio, which is backward.
# Once cloudio has been turned into a proper plugin for idbdatafile,
# we should be able to reverse the dependency like so:
<<<<<<< HEAD
<<<<<<< HEAD
target_link_libraries(cloudio idbdatafile messageqcpp loggingcpp)
=======
# target_link_libraries(cloudio idbdatafile messageqcpp loggingcpp)
>>>>>>> acb46461... Wrote a component test, which has a stand-in server thread
=======
target_link_libraries(cloudio idbdatafile messageqcpp loggingcpp)
>>>>>>> d97a570b... Made the cloud IO lib a plugin to the IDB filesystem stuff.

set_target_properties(cloudio PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS cloudio DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

add_executable(cloudio_component_test component_test.cpp)

<<<<<<< HEAD
add_executable(end_to_end_test end_to_end_test.cpp)

=======
>>>>>>> acb46461... Wrote a component test, which has a stand-in server thread
# see the comment above and change this dependency to cloudio.  Hm
# our lib dependencies seem not to be declared.  Punting on that,
# maybe in the future we can have some poor unfortunate intern
# untangle all of that and declare lib dependencies properly.
# For now I'm going to do like the other executables, which means
# nearly everything AFAICT.
<<<<<<< HEAD
<<<<<<< HEAD
target_link_libraries(cloudio_component_test ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS} cloudio)
target_link_libraries(end_to_end_test ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS} cloudio)
#target_link_libraries(cloudio_component_test cloudio)
# Copy end_to_end_test dataFile to binary directory
FILE(COPY testData DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/)
=======
target_link_libraries(cloudio_component_test ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS})
>>>>>>> acb46461... Wrote a component test, which has a stand-in server thread
=======
target_link_libraries(cloudio_component_test ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS} cloudio)
#target_link_libraries(cloudio_component_test cloudio)
>>>>>>> d97a570b... Made the cloud IO lib a plugin to the IDB filesystem stuff.

