
cs_add_executable(ExeMgr main.cpp activestatementcounter.cpp femsghandler.cpp )

target_link_libraries(ExeMgr 
  PRIVATE 
    Boost::filesystem
    Boost::system
    Boost::thread
    crashtrace
    mariadb
    brm
    common_h
    dataconvert_h
    funcexp_h
    idbboot_h
    joblist_h
    querystats_h
    querytele_h
    rowgroup_h
    threadpool_h
    udfsdk_h
    )



########### install files ###############

install(TARGETS ExeMgr DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)
