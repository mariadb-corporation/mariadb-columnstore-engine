
add_executable(ExeMgr main.cpp activestatementcounter.cpp femsghandler.cpp )

target_link_libraries(ExeMgr 
  PRIVATE 
    Boost::filesystem
    Boost::system
    Boost::thread
    crashtrace
    mariadb
    alarmmanager_h
    brm
    common_h
    configcpp
    dataconvert_h
    execplan
    funcexp_h
    idbboot_h
    joblist_h
    loggingcpp_h
    messageqcpp_h
    oamcpp
    querystats_h
    querytele_h
    rowgroup_h
    rwlock_h
    threadpool_h
    udfsdk_h
    )

target_pch(ExeMgr)


########### install files ###############

install(TARGETS ExeMgr DESTINATION ${ENGINE_BINDIR} COMPONENT platform)
