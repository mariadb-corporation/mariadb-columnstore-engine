
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
  PUBLIC 
  PRIVATE
    alarmmanager
    batchloader
    Boost::system
    brm
    common_h
    configcpp_h
    dataconvert_h
    idbboot_h
    idbdatafile_h
    joblist_h
    LibXml2::LibXml2
    loggingcpp
    messageqcpp_h
    oamcpp_h
    we_server_h
    execplan
    we_shared_h
    rwlock_h
    writeengine)

install(TARGETS cpimport DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

