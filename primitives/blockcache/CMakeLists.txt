
target_include_directories(dbbc_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(dbbc STATIC 
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
  PUBLIC dbbc_h
  PRIVATE alarmmanager_h
          brm_h
          common_h
          compress_h
          configcpp_h
          idbdatafile_h
          joblist_h
          loggingcpp_h
          messageqcpp_h 
          oamcpp_h
          primproc_h
          we_shared_h
          we_wrapper_h)

target_include_directories(dbbc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_pch(dbbc)

INSTALL (TARGETS dbbc DESTINATION ${ENGINE_LIBDIR})
