CMAKE_MINIMUM_REQUIRED (VERSION 3.10)

PROJECT (storagemanager)

INCLUDE_DIRECTORIES (include ${CMAKE_BINARY_DIR}/include ${ENGINE_UTILS_COMMON_INCLUDE} ${S3API_DIR})

SET (
  storagemanager_SRCS
  src/AppendTask.cpp
  src/ClientRequestProcessor.cpp
  src/ListDirectoryTask.cpp
  src/OpenTask.cpp
  src/PingTask.cpp
  src/PosixTask.cpp
  src/ProcessTask.cpp
  src/ReadTask.cpp
  src/StatTask.cpp
  src/ThreadPool.cpp
  src/TruncateTask.cpp
  src/UnlinkTask.cpp
  src/WriteTask.cpp
  src/CopyTask.cpp
  src/IOCoordinator.cpp
  src/SessionManager.cpp
  src/Config.cpp
  src/CloudStorage.cpp
  src/S3Storage.cpp
  src/LocalStorage.cpp
  src/Cache.cpp
  src/SMLogging.cpp
  src/Downloader.cpp
  src/Synchronizer.cpp
  src/RWLock.cpp
  src/MetadataFile.cpp
  src/Replicator.cpp
  src/Utilities.cpp
  src/Ownership.cpp
  src/PrefixCache.cpp
  src/SyncTask.cpp
  src/ListIOTask.cpp
  src/TerminateIOTask.cpp
  ../utils/common/crashtrace.cpp
)

OPTION (SM_TRACE "Enable some tracing output" OFF)
OPTION (SM_LOG_TO_STDERR "Make the logging system also print to stderr" OFF)
IF (SM_TRACE)
  ADD_DEFINITIONS (-DSM_TRACE)
ENDIF ()

IF (CMAKE_BUILD_TYPE STREQUAL Debug)
  ADD_DEFINITIONS (-DDEBUG -Werror -Wall)
  SET (S3_CONFIGURE_OPT --enable-debug)
ENDIF ()

IF (SM_LOG_TO_STDERR)
  ADD_DEFINITIONS (-DDEBUG)
ENDIF ()

CONFIGURE_FILE (
  "${CMAKE_CURRENT_SOURCE_DIR}/storagemanager.cnf.in" "${CMAKE_CURRENT_SOURCE_DIR}/storagemanager.cnf" @ONLY
)

# get linkage right
LINK_DIRECTORIES (${CMAKE_BINARY_DIR}/lib)
SET (CMAKE_INSTALL_RPATH $ORIGIN $ORIGIN/../lib)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (storagemanager STATIC ${storagemanager_SRCS})
ELSE ()
  ADD_LIBRARY (storagemanager SHARED ${storagemanager_SRCS})
ENDIF ()

ADD_DEPENDENCIES (storagemanager marias3 external_boost)

TARGET_COMPILE_DEFINITIONS (storagemanager PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)
TARGET_LINK_LIBRARIES (
  storagemanager
  boost_chrono
  boost_system
  boost_thread
  boost_filesystem
  boost_regex
  pthread
  ${S3API_DEPS}
)

ADD_EXECUTABLE (StorageManager src/main.cpp)
TARGET_LINK_LIBRARIES (StorageManager storagemanager)
SET_PROPERTY (TARGET StorageManager PROPERTY CXX_STANDARD 20)

SET (TMPDIR ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
SET (CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

ADD_EXECUTABLE (unit_tests src/unit_tests.cpp)
TARGET_COMPILE_DEFINITIONS (unit_tests PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)
TARGET_LINK_LIBRARIES (unit_tests storagemanager)

ADD_EXECUTABLE (testS3Connection src/testS3Connection.cpp)
TARGET_COMPILE_DEFINITIONS (testS3Connection PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)
TARGET_LINK_LIBRARIES (testS3Connection storagemanager)

SET (CMAKE_RUNTIME_OUTPUT_DIRECTORY ${TMPDIR})

ADD_CUSTOM_COMMAND (
  TARGET unit_tests
  PRE_BUILD
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/test_data
)
ADD_CUSTOM_COMMAND (
  TARGET unit_tests
  POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/test_data
          ${CMAKE_CURRENT_BINARY_DIR}/test_data
)

# The includes and lib linkages required to link against cloudio ... pretty crazy.  When lib dependencies are eventually
# config'd right, change this to only include and link against cloudio.
INCLUDE_DIRECTORIES (${ENGINE_SRC_DIR}/utils/cloudio ${ENGINE_COMMON_INCLUDES})
ADD_EXECUTABLE (smcat src/smcat.cpp)
TARGET_LINK_LIBRARIES (smcat storagemanager cloudio ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

ADD_EXECUTABLE (smput src/smput.cpp)
TARGET_LINK_LIBRARIES (smput storagemanager cloudio ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

ADD_EXECUTABLE (smls src/smls.cpp)
TARGET_LINK_LIBRARIES (smls storagemanager cloudio ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

ADD_EXECUTABLE (smrm src/smrm.cpp)
TARGET_LINK_LIBRARIES (smrm storagemanager cloudio ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

ADD_EXECUTABLE (smps src/smps.cpp)
TARGET_LINK_LIBRARIES (smps storagemanager cloudio ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

ADD_EXECUTABLE (smkill src/smkill.cpp)
TARGET_LINK_LIBRARIES (smkill storagemanager cloudio ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})
INSTALL (TARGETS storagemanager LIBRARY DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

INSTALL (
  TARGETS StorageManager
          smcat
          smkill
          smps
          smput
          smls
          smrm
          testS3Connection
  RUNTIME DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine
)

INSTALL (
  FILES storagemanager.cnf
  DESTINATION ${ENGINE_SYSCONFDIR}/columnstore
  COMPONENT columnstore-engine
)

INSTALL (
  FILES storagemanager.cnf
  RENAME storagemanager.cnf.example
  DESTINATION ${ENGINE_SYSCONFDIR}/columnstore
  COMPONENT columnstore-engine
)
