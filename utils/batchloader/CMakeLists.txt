
add_library(batchloader SHARED batchloader.cpp)

target_include_directories(batchloader PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(batchloader 
  PRIVATE alarmmanager_h 
          brm_h
          common_h 
          configcpp_h
          joblist_h
          loggingcpp_h 
          messageqcpp_h 
          oamcpp_h )

set_target_properties(batchloader PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS batchloader DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


