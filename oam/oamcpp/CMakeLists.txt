target_include_directories(oamcpp_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(oamcpp STATIC liboamcpp.cpp oamcache.cpp)

target_link_libraries(oamcpp 
  INTERFACE oamcpp_h
  PRIVATE
    messageqcpp
    configcpp_h
    alarmmanager
    brm_h
    execplan_h
    joblist_h
    loggingcpp_h
    rwlock_h
    common_h
    idbboot_h
    idbdatafile_h
  )
