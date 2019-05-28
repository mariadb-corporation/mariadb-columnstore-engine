target_include_directories(common_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(common SHARED 
    fixedallocator.cpp
    poolallocator.cpp
    cgroupconfigurator.cpp
    MonitorProcMem.cpp
    nullvaluemanip.cpp
    threadnaming.cpp)

target_include_directories(common PUBLIC common_h)

set_target_properties(common PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_link_libraries(common 
  PUBLIC common_h
  PRIVATE configcpp_h 
          execplan_h 
          joblist_h
          loggingcpp_h 
          messageqcpp_h 
          )

target_pch(common pch.h)

install(TARGETS common DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

