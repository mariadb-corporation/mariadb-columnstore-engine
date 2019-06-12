
target_include_directories(writeengineclient_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})


cs_add_library(writeengineclient SHARED we_clients.cpp we_ddlcommandclient.cpp we_dmlcommandclient.cpp)

target_link_libraries(writeengineclient 
  INTERFACE writeengineclient_h 
  PRIVATE    
    alarmmanager_h
    brm_h
    common_h
    compress_h
    configcpp_h
    dataconvert_h
    ddlpackage_h
    ddlpackageproc_h
    execplan_h
    idbboot_h
    idbdatafile_h
    joblist_h
    loggingcpp_h
    messageqcpp
    oamcpp_h
    rwlock_h
    we_server_h
    we_shared_h
    writeengine_h
    )

set_target_properties(writeengineclient PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS writeengineclient DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libs)


