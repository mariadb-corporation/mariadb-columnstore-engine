
cs_add_executable(dbbuilder dbbuilder.cpp systemcatalog.cpp)

target_link_libraries(dbbuilder 
  PRIVATE 
    alarmmanager_h
    brm_h 
    common_h
    compress_h
    configcpp
    dataconvert_h
    ddlpackage_h
    ddlpackageproc_h 
    dmlpackage_h
    dmlpackageproc_h
    execplan
    idbboot
    idbdatafile
    joblist_h
    loggingcpp
    mariadb
    messageqcpp_h 
    oamcpp
    querystats_h
    querytele_h
    rowgroup_h
    rwlock_h
    threadpool_h
    udfsdk_h
    writeengineclient_h
    writeengine
    we_shared_h
    Boost::system
  )

install(TARGETS dbbuilder DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)

