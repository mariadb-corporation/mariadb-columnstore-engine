
cs_add_executable(cpimport 
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
    batchloader
    brm
    common_h
    dataconvert_h
    idbboot_h
    joblist_h
    LibXml2::LibXml2
    Boost::system
    Boost::filesystem
    Boost::thread
    we_server_h
    we_shared_h
    writeengine)

install(TARGETS cpimport DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

