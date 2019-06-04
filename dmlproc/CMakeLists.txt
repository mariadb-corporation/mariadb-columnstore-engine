
add_executable(DMLProc 
    dmlproc.cpp
    dmlprocessor.cpp
    dmlresultbuffer.cpp
    batchinsertprocessor.cpp
    )


target_link_libraries(DMLProc 
  PRIVATE
    rwlock_h
    alarmmanager
    batchloader
    brm
    cacheutils_h
    common_h
    compress_h
    configcpp
    crashtrace
    dataconvert_h
    ddlcleanuputil
    dmlpackage
    dmlpackageproc
    funcexp_h
    idbboot_h
    idbdatafile_h
    joblist_h
    loggingcpp
    mariadb
    messageqcpp
    execplan
    oamcpp
    querystats_h
    querytele_h
    rowgroup_h
    threadpool_h
    udfsdk_h
    we_server_h
    we_shared_h
    writeengine
    Boost::system
    Boost::thread

    )

target_pch(DMLProc)

install(TARGETS DMLProc DESTINATION ${ENGINE_BINDIR} COMPONENT platform)



