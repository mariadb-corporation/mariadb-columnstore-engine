
cs_add_executable(DDLProc ddlproc.cpp ddlprocessor.cpp )

target_link_libraries(DDLProc
  PRIVATE 
    alarmmanager_h
    brm_h
    cacheutils_h
    common_h
    compress_h
    configcpp_h
    dataconvert_h
    ddlpackage
    ddlpackageproc
    execplan_h
    funcexp_h
    idbboot_h
    idbdatafile_h
    joblist
    loggingcpp
    messageqcpp_h
    oamcpp_h
    querytele_h
    rwlock_h
    threadpool_h
    writeengineclient_h
    we_server_h
    we_shared_h
    writeengine
    crashtrace
    Boost::thread
    )


install(TARGETS DDLProc DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

