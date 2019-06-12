target_include_directories(batchloader_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(batchloader SHARED batchloader.cpp)


target_link_libraries(batchloader 
  INTERFACE batchloader_h
  PUBLIC
  PRIVATE     
    alarmmanager_h 
    brm_h
    common_h 
    configcpp_h
    execplan_h
    joblist_h
    loggingcpp_h 
    messageqcpp_h
    oamcpp_h
    rwlock_h
    )

set_target_properties(batchloader PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS batchloader DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libs)


