
add_executable(cpimport 
    we_splitterapp.cpp
    we_cmdargs.cpp
    we_sdhandler.cpp
    we_respreadthread.cpp
    we_filereadthread.cpp
    we_splclient.cpp
    we_brmupdater.cpp
    we_tablelockgrabber.cpp
    we_xmlgetter.cpp)


target_link_libraries(cpimport 
  PRIVATE alarmmanager_h
          batchloader
          brm_h
          common_h
          configcpp_h
          dataconvert_h
          idbboot_h
          idbdatafile_h
          joblist_h
          LibXml2::LibXml2
          loggingcpp_h
          messageqcpp_h
          oamcpp_h
          we_server_h
          we_shared_h)

install(TARGETS cpimport DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

