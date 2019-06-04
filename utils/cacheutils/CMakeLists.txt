target_include_directories(cacheutils_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(cacheutils SHARED cacheutils.cpp)

target_link_libraries(cacheutils
  PRIVATE 
    cacheutils_h
    brm_h
    configcpp_h
    common_h
    execplan_h
    joblist_h
    loggingcpp_h
    messageqcpp_h
    Boost::thread
    )

set_target_properties(cacheutils PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_include_directories(cacheutils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

install(TARGETS cacheutils DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

