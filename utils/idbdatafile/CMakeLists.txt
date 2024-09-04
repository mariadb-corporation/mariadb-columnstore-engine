INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES} ../cloudio)

# ########## next target ###############

SET (
  idbdatafile_LIB_SRCS
  BufferedFile.cpp
  IDBDataFile.cpp
  IDBFactory.cpp
  IDBFileSystem.cpp
  IDBLogger.cpp
  IDBPolicy.cpp
  PosixFileSystem.cpp
  UnbufferedFile.cpp
)

ADD_LIBRARY (idbdatafile SHARED ${idbdatafile_LIB_SRCS})

TARGET_LINK_LIBRARIES (idbdatafile ${NETSNMP_LIBRARIES} ${ENGINE_OAM_LIBS} boost_filesystem boost_system)

TARGET_COMPILE_DEFINITIONS (idbdatafile PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)

INSTALL (
  TARGETS idbdatafile
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
