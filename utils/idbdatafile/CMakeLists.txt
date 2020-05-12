
include_directories( ${ENGINE_COMMON_INCLUDES} ../cloudio)

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

target_link_libraries(idbdatafile ${NETSNMP_LIBRARIES} ${ENGINE_OAM_LIBS})

target_compile_definitions(idbdatafile PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)

install(TARGETS idbdatafile DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
