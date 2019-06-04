
add_executable(ddlcleanup ddlcleanup.cpp)

target_link_libraries(ddlcleanup 
  PRIVATE 
    alarmmanager_h
    brm_h
    common_h
    compress_h
    configcpp_h
    dataconvert_h
    ddlcleanuputil
    ddlpackage_h
    ddlpackageproc_h
    execplan_h
    idbboot_h
    idbdatafile_h
    joblist_h
    loggingcpp_h
    messageqcpp_h
    oamcpp_h
    rwlock_h
    writeengineclient_h
    we_shared_h
    writeengine_h
    Boost::system
  )

install(TARGETS ddlcleanup DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

