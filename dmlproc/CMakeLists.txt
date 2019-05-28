
cs_add_executable(DMLProc 
    dmlproc.cpp
    dmlprocessor.cpp
    dmlresultbuffer.cpp
    batchinsertprocessor.cpp
    )


target_link_libraries(DMLProc 
  PRIVATE
    batchloader
    brm
    cacheutils_h
    common_h
    crashtrace
    dataconvert_h
    ddlcleanuputil
    dmlpackage
    dmlpackageproc
    funcexp_h
    idbboot_h
    joblist_h
    mariadb
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


install(TARGETS DMLProc DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)



