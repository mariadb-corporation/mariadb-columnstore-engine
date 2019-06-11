
target_include_directories(dbbc_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(dbbc STATIC 
    blockcacheclient.cpp
    blockrequestprocessor.cpp
    fileblockrequestqueue.cpp
    filebuffer.cpp
    filebuffermgr.cpp
    filerequest.cpp
    iomanager.cpp
    stats.cpp
    fsutils.cpp)


target_link_libraries(dbbc
  INTERFACE dbbc_h
  PRIVATE 
    alarmmanager_h
    brm_h
    common_h
    compress_h
    configcpp_h
    execplan_h
    idbdatafile_h
    joblist_h
    loggingcpp_h
    messageqcpp_h 
    oamcpp_h
    primproc_h
    rwlock
    writeengine_h
    we_shared_h
    )

target_include_directories(dbbc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})


INSTALL (TARGETS dbbc DESTINATION ${ENGINE_LIBDIR})
