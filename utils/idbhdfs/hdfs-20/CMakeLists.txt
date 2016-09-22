
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(hdfs-20_LIB_SRCS 
    ${CMAKE_CURRENT_SOURCE_DIR}/../hdfs-shared/HdfsFile.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../hdfs-shared/HdfsFileSystem.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../hdfs-shared/HdfsFsCache.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../hdfs-shared/HdfsRdwrFileBuffer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../hdfs-shared/HdfsRdwrMemBuffer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../hdfs-shared/HdfsPlugin.cpp)

add_definitions(-DCDH4)

add_library(hdfs-20 SHARED ${hdfs-20_LIB_SRCS})

set_target_properties(hdfs-20 PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS hdfs-20 DESTINATION ${ENGINE_LIBDIR})

install(PROGRAMS setenv-hdfs-20 DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

