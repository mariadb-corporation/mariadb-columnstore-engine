target_include_directories(dmlpackageproc_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(dmlpackageproc SHARED 
    deletepackageprocessor.cpp
    dmlpackageprocessor.cpp
    insertpackageprocessor.cpp
    updatepackageprocessor.cpp
    commandpackageprocessor.cpp
    autoincrementdata.cpp
    tablelockdata.cpp)


target_link_libraries(dmlpackageproc 
  INTERFACE dmlpackageproc_h
  PRIVATE
    common_h
    dataconvert_h
    dmlpackage_h
    funcexp_h
    idbboot_h
    joblist_h
    mariadb
    querystats_h
    querytele_h
    rowgroup_h
    threadpool_h
    udfsdk_h
    writeengineclient_h
    we_server_h
    we_shared_h
    writeengine_h
 PUBLIC
    ddlpackage
    dmlpackage
    brm
    writeengine
    writeengineredistribute
    )

set_target_properties(dmlpackageproc PROPERTIES VERSION 1.0.0 SOVERSION 1)


install(TARGETS dmlpackageproc DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libs)


