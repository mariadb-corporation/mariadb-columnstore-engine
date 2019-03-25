cmake_minimum_required(VERSION 2.8.12)
project(storagemanager)

include_directories(include)

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
)

option(TRACE "Enable some tracing output" OFF)
if (TRACE)
    add_definitions(-DSM_TRACE)
endif()

if (CMAKE_BUILD_TYPE STREQUAL Debug)
    add_definitions(-DDEBUG)
    set(S3_CONFIGURE_OPT --enable-debug)
endif()

set(S3API_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libmarias3)

include(ExternalProject)
ExternalProject_Add(marias3
    SOURCE_DIR ${S3API_DIR}
    CONFIGURE_COMMAND autoreconf -fi ${S3API_DIR} && ${S3API_DIR}/configure --enable-shared=no --prefix=${CMAKE_CURRENT_BINARY_DIR} ${S3_CONFIGURE_OPT}
    BUILD_COMMAND make
    BUILD_IN_SOURCE 0
    INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}
)
link_directories(${CMAKE_CURRENT_BINARY_DIR}/lib)

add_executable(StorageManager src/main.cpp ${storagemanager_SRCS})
add_dependencies(StorageManager marias3)
target_link_libraries(StorageManager boost_system boost_thread boost_filesystem boost_regex libmarias3.a)
set_property(TARGET StorageManager PROPERTY CXX_STANDARD 11)

add_executable(unit_tests src/unit_tests.cpp ${storagemanager_SRCS})
add_dependencies(unit_tests marias3)
target_link_libraries(unit_tests boost_system boost_thread boost_filesystem boost_regex libmarias3.a)
set_property(TARGET unit_tests PROPERTY CXX_STANDARD 11)

#install(TARGETS StorageManager DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


