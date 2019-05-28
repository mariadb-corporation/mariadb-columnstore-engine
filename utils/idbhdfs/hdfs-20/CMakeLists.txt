

cs_add_library(hdfs-20 SHARED     
    ../hdfs-shared/HdfsFile.cpp
    ../hdfs-shared/HdfsFileSystem.cpp
    ../hdfs-shared/HdfsFsCache.cpp
    ../hdfs-shared/HdfsRdwrFileBuffer.cpp
    ../hdfs-shared/HdfsRdwrMemBuffer.cpp
    ../hdfs-shared/HdfsPlugin.cpp)


target_compile_definitions(hdfs-20 PRIVATE CDH4)

target_link_libraries(hdfs-20 
  PRIVATE 
    idbdatafile_h
    messageqcpp_h
    compress_h
    loggingcpp_h
    common_h
    configcpp_h)

set_target_properties(hdfs-20 PROPERTIES VERSION 1.0.0 SOVERSION 1)


install(TARGETS hdfs-20 DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(PROGRAMS setenv-hdfs-20 DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)

