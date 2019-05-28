target_include_directories(oamcpp_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(oamcpp SHARED liboamcpp.cpp oamcache.cpp)

target_link_libraries(oamcpp 
  PUBLIC oamcpp_h 
  PRIVATE alarmmanager_h
          brm_h
          columnstore_h
          common_h
          configcpp_h
          ddlpackage_h 
          idbboot_h
          idbdatafile_h
          joblist_h
          loggingcpp_h
          messageqcpp_h )

set_target_properties(oamcpp PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS oamcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


