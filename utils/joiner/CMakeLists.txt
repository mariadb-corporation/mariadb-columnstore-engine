
add_library(joiner SHARED 
  joiner.cpp 
  tuplejoiner.cpp 
  joinpartition.cpp)

target_link_libraries(joiner 
  PRIVATE brm_h 
          common_h
          compress_h
          configcpp_h
          dataconvert_h
          execplan_h 
          idbboot_h
          joblist_h 
          loggingcpp_h
          mariadb
          messageqcpp_h
          rowgroup_h 
          rwlock_h
          udfsdk_h)

target_include_directories(joiner PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

set_target_properties(joiner PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_pch(joiner)

install(TARGETS joiner DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


