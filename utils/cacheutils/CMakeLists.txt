target_include_directories(cacheutils_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(cacheutils SHARED cacheutils.cpp)

target_link_libraries(cacheutils
  PUBLIC cacheutils_h
  PRIVATE brm_h
          configcpp_h
          common_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          )

set_target_properties(cacheutils PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_include_directories(cacheutils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

install(TARGETS cacheutils DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

