include_directories(${ENGINE_COMMON_INCLUDES} storage-manager/include)

set(cloudio_LIB_SRCS SMComm.cpp SMDataFile.cpp SMFileFactory.cpp SMFileSystem.cpp SocketPool.cpp)

add_library(cloudio SHARED ${cloudio_LIB_SRCS})

# IDBDataFile currently depends on cloudio, which is backward.
# Once cloudio has been turned into a proper plugin for idbdatafile,
# we should be able to reverse the dependency like so:
# target_link_libraries(cloudio idbdatafile messageqcpp loggingcpp)

set_target_properties(cloudio PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS cloudio DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

add_executable(cloudio_component_test component_test.cpp)

# see the comment above and change this dependency to cloudio.  Hm
# our lib dependencies seem not to be declared.  Punting on that,
# maybe in the future we can have some poor unfortunate intern
# untangle all of that and declare lib dependencies properly.
# For now I'm going to do like the other executables, which means
# nearly everything AFAICT.
target_link_libraries(cloudio_component_test ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS})

