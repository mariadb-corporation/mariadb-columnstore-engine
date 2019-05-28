target_include_directories(dmlpackageproc_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(dmlpackageproc SHARED 
    deletepackageprocessor.cpp
    dmlpackageprocessor.cpp
    insertpackageprocessor.cpp
    updatepackageprocessor.cpp
    commandpackageprocessor.cpp
    autoincrementdata.cpp
    tablelockdata.cpp)


target_link_libraries(dmlpackageproc 
  PUBLIC dmlpackageproc_h
  PRIVATE alarmmanager_h
          brm_h
          common_h
          compress_h
          configcpp_h
          dataconvert_h
          dmlpackage_h
          execplan_h 
          funcexp_h
          idbboot_h
          idbdatafile_h
          joblist_h
          loggingcpp_h
          mariadb
          messageqcpp_h 
          oamcpp_h
          querystats_h
          querytele_h
          rowgroup_h
          threadpool_h
          udfsdk_h
          we_client_h
          we_shared_h 
          we_server_h
          we_wrapper_h)

set_target_properties(dmlpackageproc PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_pch(dmlpackageproc)

install(TARGETS dmlpackageproc DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


