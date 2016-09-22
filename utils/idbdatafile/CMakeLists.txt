
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(idbdatafile_LIB_SRCS
    BufferedFile.cpp
    IDBDataFile.cpp
    IDBFactory.cpp
    IDBFileSystem.cpp
    IDBLogger.cpp
    IDBPolicy.cpp
    PosixFileSystem.cpp
    UnbufferedFile.cpp)

add_library(idbdatafile SHARED ${idbdatafile_LIB_SRCS})

target_link_libraries(idbdatafile ${NETSNMP_LIBRARIES})

set_target_properties(idbdatafile PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS idbdatafile DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)
