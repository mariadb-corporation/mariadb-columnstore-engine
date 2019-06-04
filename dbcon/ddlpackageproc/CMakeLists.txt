
target_include_directories(ddlpackageproc_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(ddlpackageproc SHARED 
    ddlpackageprocessor.cpp
    createtableprocessor.cpp
    altertableprocessor.cpp
    droptableprocessor.cpp
    markpartitionprocessor.cpp
    restorepartitionprocessor.cpp
    droppartitionprocessor.cpp)


target_link_libraries(ddlpackageproc 
  INTERFACE ddlpackageproc_h 
  PRIVATE
    alarmmanager_h
    common_h
    compress_h
    configcpp_h
    dataconvert_h
    dmlpackage_h 
    funcexp_h
    idbboot_h
    joblist_h
    loggingcpp_h
    mariadb
    messageqcpp_h
    querystats_h
    querytele_h
    rowgroup_h
    rwlock_h
    threadpool_h
    udfsdk_h
    writeengine_h
    we_server_h
    we_shared_h
  PUBLIC
    execplan
    cacheutils
    ddlpackage
    idbdatafile
    writeengineclient
    writeengine
    oamcpp
    brm
    Boost::regex
)

set_target_properties(ddlpackageproc PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_pch(ddlpackageproc)

install(TARGETS ddlpackageproc DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

