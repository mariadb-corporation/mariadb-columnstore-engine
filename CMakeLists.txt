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
endif()

add_executable(StorageManager src/main.cpp ${storagemanager_SRCS})
target_link_libraries(StorageManager boost_system boost_thread boost_filesystem boost_regex)

add_executable(unit_tests src/unit_tests.cpp ${storagemanager_SRCS})
target_link_libraries(unit_tests boost_system boost_thread boost_filesystem boost_regex)

#install(TARGETS StorageManager DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


