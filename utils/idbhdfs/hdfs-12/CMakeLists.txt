
cs_add_library(hdfs-12 SHARED 
    ../hdfs-shared/HdfsFile.cpp
    ../hdfs-shared/HdfsFileSystem.cpp
    ../hdfs-shared/HdfsFsCache.cpp
    ../hdfs-shared/HdfsRdwrFileBuffer.cpp
    ../hdfs-shared/HdfsRdwrMemBuffer.cpp
    ../hdfs-shared/HdfsPlugin.cpp)


set_target_properties(hdfs-12 PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_link_libraries(hdfs-12 
  PRIVATE 
    idbdatafile_h
    messageqcpp_h
    compress_h
    loggingcpp_h
    common_h
    configcpp_h)


install(TARGETS hdfs-12 DESTINATION ${ENGINE_LIBDIR})

install(PROGRAMS setenv-hdfs-12 DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

