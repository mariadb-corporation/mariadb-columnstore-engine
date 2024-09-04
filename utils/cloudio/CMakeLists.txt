INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES} ${ENGINE_SRC_DIR}/storage-manager/include)

SET (
  cloudio_LIB_SRCS
  SMComm.cpp
  SMDataFile.cpp
  SMFileFactory.cpp
  SMFileSystem.cpp
  SocketPool.cpp
  cloud_plugin.cpp
  ../../datatypes/mcs_datatype.cpp
)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (cloudio STATIC ${cloudio_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (cloudio SHARED ${cloudio_LIB_SRCS})
ENDIF ()

# IDBDataFile currently depends on cloudio, which is backward. Once cloudio has been turned into a proper plugin for
# idbdatafile, we should be able to reverse the dependency like so:
TARGET_LINK_LIBRARIES (cloudio idbdatafile messageqcpp loggingcpp)

INSTALL (
  TARGETS cloudio
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)

ADD_EXECUTABLE (cloudio_component_test component_test.cpp)

ADD_EXECUTABLE (end_to_end_test end_to_end_test.cpp)

# see the comment above and change this dependency to cloudio.  Hm our lib dependencies seem not to be declared. Punting
# on that, maybe in the future we can have some poor unfortunate intern untangle all of that and declare lib
# dependencies properly. For now I'm going to do like the other executables, which means nearly everything AFAICT.
TARGET_LINK_LIBRARIES (cloudio_component_test ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS} cloudio)
TARGET_LINK_LIBRARIES (end_to_end_test ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS} cloudio)
# target_link_libraries(cloudio_component_test cloudio) Copy end_to_end_test dataFile to binary directory
FILE (COPY testData DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/)
