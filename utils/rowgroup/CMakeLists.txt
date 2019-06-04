
target_include_directories(rowgroup_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})


add_library(rowgroup SHARED rowaggregation.cpp rowgroup.cpp)

target_link_libraries(rowgroup 
  INTERFACE rowgroup_h
  PRIVATE
    joblist_h 
    execplan_h 
    brm_h 
    udfsdk_h 
    funcexp_h 
    oamcpp_h 
    configcpp_h
    loggingcpp_h 
    dataconvert_h
    idbboot_h
    rwlock_h
    messageqcpp_h
    common_h
    mariadb
    alarmmanager_h
  PUBLIC
    joblist
    )

set_target_properties(rowgroup PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS rowgroup DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

