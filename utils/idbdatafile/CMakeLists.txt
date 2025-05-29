include_directories(${ENGINE_COMMON_INCLUDES} ../cloudio)

# ########## next target ###############

set(idbdatafile_LIB_SRCS
    BufferedFile.cpp
    IDBDataFile.cpp
    IDBFactory.cpp
    IDBFileSystem.cpp
    IDBLogger.cpp
    IDBPolicy.cpp
    PosixFileSystem.cpp
    UnbufferedFile.cpp
)

columnstore_library(idbdatafile ${idbdatafile_LIB_SRCS})
columnstore_link(idbdatafile PRIVATE ${ENGINE_OAM_LIBS} boost_filesystem boost_system compress)
