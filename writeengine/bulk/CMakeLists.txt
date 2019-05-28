
add_library(we_bulk STATIC 
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
  PUBLIC we_wrapper_h
  PRIVATE alarmmanager_h
          brm_h 
          cacheutils_h
          common_h
          compress_h
          configcpp_h
          dataconvert_h
          funcexp_h
          idbboot_h
          idbdatafile_h 
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h
          querytele_h
          threadpool_h
          we_client_h
          we_server_h
          we_shared_h
          we_xml_h)

target_compile_definitions(we_bulk PRIVATE _FILE_OFFSET_BITS=64)


# cpimport.bin

add_executable(cpimport.bin cpimport.cpp)

target_link_libraries(cpimport.bin
  PRIVATE we_bulk)
#[[
  PRIVATE idbdatafile_h 
          messageqcpp_h 
          we_shared_h 
          execplan_h 
          we_wrapper_h 
          brm_h 
          compress_h 
          we_xml_h 
          querytele_h 
          oamcpp_h
          funcexp_h)
]]
install(TARGETS cpimport.bin DESTINATION ${ENGINE_BINDIR} COMPONENT platform)
