
add_executable(DDLProc ddlproc.cpp ddlprocessor.cpp ../utils/common/crashtrace.cpp)

target_link_libraries(DDLProc
  PRIVATE alarmmanager_h
          brm_h
          cacheutils_h
          common_h
          compress_h
          configcpp_h
          dataconvert_h
          ddlpackage_h
          ddlpackageproc_h
          execplan_h
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
          we_wrapper_h)

target_pch(DDLProc)

install(TARGETS DDLProc DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

