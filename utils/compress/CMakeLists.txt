
target_include_directories(compress_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(compress SHARED 
    idbcompress.cpp
    version1.cpp)

target_link_libraries(compress 
  PUBLIC compress_h 
  PRIVATE brm_h
          columnstore_h
          common_h 
          idbboot_h
          loggingcpp_h
          rt
          snappy)

set_target_properties(compress PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_pch(compress)

install(TARGETS compress DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

