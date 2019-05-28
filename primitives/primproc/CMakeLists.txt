
target_include_directories(primproc_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})


add_executable(PrimProc 
    primproc.cpp
    batchprimitiveprocessor.cpp
    bppseeder.cpp
    bppsendthread.cpp
    columncommand.cpp
    command.cpp
    dictstep.cpp
    filtercommand.cpp
    logger.cpp
    passthrucommand.cpp
    primitiveserver.cpp
    pseudocc.cpp
    rtscommand.cpp
    umsocketselector.cpp
    ../../utils/common/crashtrace.cpp)

target_link_libraries(PrimProc 
  PUBLIC primproc_h
  PRIVATE alarmmanager_h          
          Boost::system
          Boost::thread
          brm_h
          common_h
          compress_h
          configcpp_h
          dataconvert_h
          dbbc_h
          execplan_h
          funcexp_h
          idbboot_h
          idbdatafile_h
          joblist_h
          joiner_h
          loggingcpp_h
          mariadb
          messageqcpp_h
          oamcpp_h
          processor_h
          rowgroup_h
          threadpool_h
          udfsdk_h
          we_client_h
          we_shared_h
          we_wrapper_h)

target_pch(PrimProc)


install(TARGETS PrimProc DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


