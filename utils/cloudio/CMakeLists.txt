include_directories(${ENGINE_COMMON_INCLUDES} storage-manager/include)

set(cloudio_LIB_SRCS SMComm.cpp SMDataFile.cpp SMFileFactory.cpp SMFileSystem.cpp SocketPool.cpp)

add_library(cloudio SHARED ${cloudio_LIB_SRCS})

set_target_properties(cloudio PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS cloudio DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

