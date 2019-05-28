
add_executable(DMLProc 
    dmlproc.cpp
    dmlprocessor.cpp
    dmlresultbuffer.cpp
    batchinsertprocessor.cpp
    ../utils/common/crashtrace.cpp)


target_link_libraries(DMLProc 
  PRIVATE alarmmanager_h
          batchloader_h
          brm_h
          cacheutils_h
          common_h
          compress_h
          configcpp_h
          dataconvert_h
          ddlcleanuputil
          dmlpackage_h
          dmlpackageproc_h
          funcexp_h
          idbboot_h
          idbdatafile_h
          joblist_h
          loggingcpp_h
          mariadb
          messageqcpp_h
          oamcpp_h
          querystats_h
          querytele_h
          rowgroup_h
          threadpool_h
          udfsdk_h
          we_shared_h
          we_server_h
          we_wrapper_h)

target_pch(DMLProc)

install(TARGETS DMLProc DESTINATION ${ENGINE_BINDIR} COMPONENT platform)



