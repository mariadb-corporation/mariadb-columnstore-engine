
target_include_directories(alarmmanager_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(alarmmanager SHARED alarmmanager.cpp alarm.cpp)

set_target_properties(alarmmanager PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_link_libraries(alarmmanager 
  INTERFACE alarmmanager_h 
  PRIVATE
    idbboot_h
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

install(TARGETS alarmmanager DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


