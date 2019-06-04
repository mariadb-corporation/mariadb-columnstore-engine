target_include_directories(ddlcleanuputil_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(ddlcleanuputil SHARED ddlcleanuputil.cpp)

target_link_libraries(ddlcleanuputil
  INTERFACE ddlcleanuputil_h
  PRIVATE
    alarmmanager_h
    brm_h
    cacheutils_h
    common_h
    compress_h
    configcpp_h
    dataconvert_h
    ddlpackage_h 
    ddlpackageproc
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

     )

set_target_properties(ddlcleanuputil PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS ddlcleanuputil DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

