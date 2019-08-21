cmake_minimum_required(VERSION 2.8.12)
project(storagemanager)

include_directories(include ${CMAKE_BINARY_DIR}/include)

set(storagemanager_SRCS 
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
)

option(SM_TRACE "Enable some tracing output" OFF)
option(SM_LOG_TO_STDERR "Make the logging system also print to stderr" OFF)
if (TRACE)
    add_definitions(-DSM_TRACE)
endif()

if (CMAKE_BUILD_TYPE STREQUAL Debug)
    add_definitions(-DDEBUG -Werror -Wall)
    set(S3_CONFIGURE_OPT --enable-debug)
endif()

if (SM_LOG_TO_STDERR)
    add_definitions(-DDEBUG)
endif()

set(S3API_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libmarias3)

include(ExternalProject)
ExternalProject_Add(ms3
    DOWNLOAD_COMMAND cd ${CMAKE_SOURCE_DIR} && git submodule update --init
    CONFIGURE_COMMAND autoreconf -fi ${S3API_DIR} && ${S3API_DIR}/configure --enable-shared --disable-static --prefix=${CMAKE_BINARY_DIR} ${S3_CONFIGURE_OPT}
    BUILD_COMMAND make
    BUILD_IN_SOURCE 0
    INSTALL_DIR ${INSTALL_ENGINE}
    EXCLUDE_FROM_ALL TRUE
)

#add_library(marias3 SHARED IMPORTED)
#set_property(TARGET marias3 PROPERTY IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libmarias3.so.3.0.2)

set(S3API_DEPS marias3 curl xml2)

# get linkage right
link_directories(${CMAKE_BINARY_DIR}/lib)
set(CMAKE_INSTALL_RPATH $ORIGIN $ORIGIN/../lib)

add_library(storagemanager SHARED ${storagemanager_SRCS})
target_compile_definitions(storagemanager PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)
target_link_libraries(storagemanager boost_system boost_thread boost_filesystem boost_regex pthread ${S3API_DEPS})
add_dependencies(storagemanager ms3)
set_property(TARGET storagemanager PROPERTY CXX_STANDARD 11)

add_executable(StorageManager src/main.cpp)
target_link_libraries(StorageManager storagemanager)
set_property(TARGET StorageManager PROPERTY CXX_STANDARD 11)

add_custom_command(
	TARGET StorageManager PRE_BUILD
	COMMAND ${CMAKE_COMMAND} -E make_directory
		${CMAKE_CURRENT_BINARY_DIR}/storage-manager/test_data)
add_custom_command(
	TARGET StorageManager POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
                ${CMAKE_SOURCE_DIR}/storage-manager/test_data
		${CMAKE_CURRENT_BINARY_DIR}/storage-manager/test_data)
add_executable(unit_tests src/unit_tests.cpp)
target_compile_definitions(unit_tests PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)
add_dependencies(unit_tests test_files)
target_link_libraries(unit_tests storagemanager)
set_property(TARGET unit_tests PROPERTY CXX_STANDARD 11)

# The includes and lib linkages required to link against cloudio ... 
# pretty crazy.  When lib dependencies are eventually config'd right,
# change this to only include and link against cloudio.
include_directories(${CMAKE_SOURCE_DIR}/utils/cloudio ${ENGINE_COMMON_INCLUDES})
add_executable(smcat src/smcat.cpp)
target_link_libraries(smcat storagemanager cloudio
    ${ENGINE_LDFLAGS}
    ${ENGINE_EXEC_LIBS}
    ${MARIADB_CLIENT_LIBS}
)

add_executable(smput src/smput.cpp)
target_link_libraries(smput storagemanager cloudio
    ${ENGINE_LDFLAGS}
    ${ENGINE_EXEC_LIBS}
    ${MARIADB_CLIENT_LIBS}
)

add_executable(smls src/smls.cpp)
target_link_libraries(smls storagemanager cloudio
    ${ENGINE_LDFLAGS}
    ${ENGINE_EXEC_LIBS}
    ${MARIADB_CLIENT_LIBS}
)

add_executable(smrm src/smrm.cpp)
target_link_libraries(smrm storagemanager cloudio
    ${ENGINE_LDFLAGS}
    ${ENGINE_EXEC_LIBS}
    ${MARIADB_CLIENT_LIBS}
)

install(TARGETS StorageManager smcat smput smls smrm storagemanager
    RUNTIME DESTINATION ${INSTALL_ENGINE}/bin
    LIBRARY DESTINATION ${INSTALL_ENGINE}/lib
    COMPONENT platform
)

install(FILES 
        ${CMAKE_BINARY_DIR}/lib/libmarias3.so
        ${CMAKE_BINARY_DIR}/lib/libmarias3.so.3
        ${CMAKE_BINARY_DIR}/lib/libmarias3.so.3.1.2
    DESTINATION ${INSTALL_ENGINE}/lib
    COMPONENT platform
)

install(FILES storagemanager.cnf 
    DESTINATION ${INSTALL_ENGINE}/etc 
    COMPONENT platform)

