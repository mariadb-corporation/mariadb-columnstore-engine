
target_include_directories(dataconvert_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(dataconvert SHARED dataconvert.cpp)


target_link_libraries(dataconvert
  PUBLIC dataconvert_h
  PRIVATE brm_h 
          common_h
          dataconvert_h 
          execplan_h 
          joblist_h 
          loggingcpp_h 
          messageqcpp_h 
          )

set_target_properties(dataconvert PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS dataconvert DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)
