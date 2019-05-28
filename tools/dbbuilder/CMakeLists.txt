
add_executable(dbbuilder dbbuilder.cpp systemcatalog.cpp)

target_link_libraries(dbbuilder 
  PRIVATE alarmmanager_h
          brm_h 
          common_h
          compress_h
          configcpp_h
          dataconvert_h
          ddlpackage_h
          ddlpackageproc_h 
          dmlpackage_h
          dmlpackageproc_h
          execplan_h
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
          we_client_h
          we_shared_h 
          we_wrapper_h 
          )

install(TARGETS dbbuilder DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

