cmake_minimum_required(VERSION 2.8.12)

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
    src/IOCoordinator.cpp
    #src/SessionManager.cpp
    src/main.cpp 

)

add_executable(StorageManager ${storagemanager_SRCS})
target_link_libraries(StorageManager boost_system boost_thread boost_filesystem)

#install(TARGETS StorageManager DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


