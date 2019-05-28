
target_include_directories(alarmmanager_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(alarmmanager SHARED alarmmanager.cpp alarm.cpp)

#target_compile_options(alarmmanager PRIVATE -Wno-unused-result)

set_target_properties(alarmmanager PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_link_libraries(alarmmanager 
  PUBLIC alarmmanager_h 
  PRIVATE oamcpp_h 
          messageqcpp_h 
          brm_h 
          loggingcpp_h
          common_h
          configcpp_h
          joblist_h)

install(TARGETS alarmmanager DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


