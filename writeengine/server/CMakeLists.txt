target_include_directories(we_server_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})


add_executable(WriteEngineServer 
    we_server.cpp
    we_readthread.cpp
    we_ddlcommandproc.cpp
    we_observer.cpp
    we_dataloader.cpp
    we_brmrprtparser.cpp
    we_dmlcommandproc.cpp
    we_cleartablelockcmd.cpp
    we_cpifeederthread.cpp
    we_getfilesizes.cpp
    ../../utils/common/crashtrace.cpp)


target_link_libraries(WriteEngineServer 
  PRIVATE alarmmanager_h
          Boost::filesystem
          Boost::thread
          brm_h
          cacheutils_h
          common_h
          compress_h
          configcpp_h
          dataconvert_h
          ddlpackage_h
          dmlpackage_h
          dmlpackageproc_h
          funcexp_h
          idbboot_h
          idbdatafile_h
          joblist_h
          querystats_h
          loggingcpp_h
          mariadb
          messageqcpp
          oamcpp_h
          querytele_h
          rowgroup_h
          threadpool_h
          udfsdk_h
          we_shared_h
          we_wrapper_h)

install(TARGETS WriteEngineServer DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

