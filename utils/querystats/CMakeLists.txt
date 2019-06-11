target_include_directories(querystats_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(querystats SHARED querystats.cpp)

set_target_properties(querystats PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_link_libraries(querystats 
  PUBLIC querystats_h
  PRIVATE
   mariadb 
          messageqcpp_h
          joblist_h
          execplan_h 
          brm_h 
          compress_h
          mysql_client_h
          configcpp_h
          loggingcpp_h
          dataconvert_h
          idbboot_h
          common_h)

install(TARGETS querystats DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

