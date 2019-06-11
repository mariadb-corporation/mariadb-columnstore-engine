
target_include_directories(writeengine_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(writeengine SHARED 
    writeengine.cpp
    we_colop.cpp
    we_colopcompress.cpp
    we_dctnrycompress.cpp
    we_tablemetadata.cpp
    ../shared/we_blockop.cpp
    ../shared/we_brm.cpp
    ../shared/we_cache.cpp
    ../shared/we_chunkmanager.cpp
    ../shared/we_config.cpp
    ../shared/we_convertor.cpp
    ../shared/we_dbfileop.cpp
    ../shared/we_define.cpp
    ../shared/we_fileop.cpp
    ../shared/we_log.cpp
    ../shared/we_stats.cpp
    ../shared/we_bulkrollbackmgr.cpp
    ../shared/we_simplesyslog.cpp
    ../shared/we_bulkrollbackfilecompressed.cpp
    ../shared/we_bulkrollbackfilecompressedhdfs.cpp
    ../shared/we_bulkrollbackfile.cpp
    ../shared/we_rbmetawriter.cpp
    ../shared/we_dbrootextenttracker.cpp
    ../shared/we_confirmhdfsdbfile.cpp
    ../dictionary/we_dctnry.cpp
    ../xml/we_xmlop.cpp
    ../xml/we_xmljob.cpp
    ../xml/we_xmlgendata.cpp
    ../xml/we_xmlgenproc.cpp)



target_link_libraries(writeengine 
  INTERFACE writeengine_h 
  PRIVATE
    Boost::date_time
    we_shared_h
    LibXml2::LibXml2
  PUBLIC
    writeengineclient
    brm
  )

set_target_properties(writeengine PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_compile_definitions(writeengine PRIVATE _FILE_OFFSET_BITS=64)


install(TARGETS writeengine DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)
