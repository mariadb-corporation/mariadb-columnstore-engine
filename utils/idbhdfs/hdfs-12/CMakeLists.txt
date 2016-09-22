
include_directories( ${ENGINE_COMMON_INCLUDES} )

                     
########### next target ###############

set(hdfs-12_LIB_SRCS 
    ../hdfs-shared/HdfsFile.cpp
    ../hdfs-shared/HdfsFileSystem.cpp
    ../hdfs-shared/HdfsFsCache.cpp
    ../hdfs-shared/HdfsRdwrFileBuffer.cpp
    ../hdfs-shared/HdfsRdwrMemBuffer.cpp
    ../hdfs-shared/HdfsPlugin.cpp)

add_library(hdfs-12 SHARED ${hdfs-12_LIB_SRCS})

set_target_properties(hdfs-12 PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS hdfs-12 DESTINATION ${ENGINE_LIBDIR})

install(PROGRAMS setenv-hdfs-12 DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

