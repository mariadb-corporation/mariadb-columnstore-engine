
target_include_directories(querytele_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(querytele 
  SHARED querytele.cpp
    queryteleclient.cpp
    querytele_constants.cpp
    querytele_types.cpp
    QueryTeleService.cpp
    queryteleprotoimpl.cpp)


set_target_properties(querytele PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_pch(querytele)

target_link_libraries(querytele 
  PUBLIC querytele_h 
  PRIVATE threadpool_h
          thrift 
          common_h)

install(TARGETS querytele DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

