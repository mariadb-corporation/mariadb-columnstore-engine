
cs_add_library(we_bulk STATIC 
    we_brmreporter.cpp
    we_bulkload.cpp
    we_bulkloadbuffer.cpp
    we_bulkstatus.cpp
    we_colopbulk.cpp
    we_colbuf.cpp
    we_colbufcompressed.cpp
    we_colbufmgr.cpp
    we_colbufmgrdctnry.cpp
    we_colbufsec.cpp
    we_colextinf.cpp
    we_columninfo.cpp
    we_columninfocompressed.cpp
    we_columnautoinc.cpp
    we_extentstripealloc.cpp
    we_tableinfo.cpp
    we_tempxmlgendata.cpp
    we_workers.cpp)

target_link_libraries(we_bulk 
  PUBLIC 
  PRIVATE
    alarmmanager_h
    brm_h 
    cacheutils_h
    common_h
    compress_h
    configcpp_h
    dataconvert_h
    execplan_h
    funcexp_h
    idbboot_h
    idbdatafile_h 
    joblist_h
    LibXml2::LibXml2
    loggingcpp_h
    messageqcpp_h
    oamcpp_h
    querytele_h
    rwlock_h
    threadpool_h
    writeengineclient_h
    writeengine_h
    we_server_h
    we_shared_h
    we_xml_h
    writeengine_h
    )

target_compile_definitions(we_bulk PRIVATE _FILE_OFFSET_BITS=64)


# cpimport.bin

cs_add_executable(cpimport.bin cpimport.cpp)

target_link_libraries(cpimport.bin
  PUBLIC 
  PRIVATE
    we_bulk
    we_shared_h
    writeengine
    brm
    we_xml_h
    LibXml2::LibXml2
    Boost::thread
    Boost::filesystem
    )


install(TARGETS cpimport.bin DESTINATION ${ENGINE_BINDIR} COMPONENT platform)
