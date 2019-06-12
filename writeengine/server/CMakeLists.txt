target_include_directories(we_server_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})


cs_add_executable(WriteEngineServer 
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
    )


target_link_libraries(WriteEngineServer
  PRIVATE
    dmlpackageproc
    brm_h
    columnstore_h
    we_server_h
    loggingcpp_h
    messageqcpp_h
    threadpool_h
    execplan_h
    joblist_h
    rwlock_h
    common_h
    configcpp_h
    dmlpackage_h
    idbdatafile_h
    ddlpackage_h
    compress_h
    oamcpp_h
    dataconvert_h
    alarmmanager_h
    idbboot_h
    querystats_h
    rowgroup_h
    udfsdk_h
    querytele_h
    funcexp_h
    cacheutils_h
    dmlpackageproc_h
    we_shared_h
    crashtrace
    mariadb
    Boost::thread
          )

install(TARGETS WriteEngineServer DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)
