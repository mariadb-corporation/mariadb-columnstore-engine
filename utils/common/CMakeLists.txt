target_include_directories(common_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(crashtrace_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(common STATIC 
    fixedallocator.cpp
    poolallocator.cpp
    cgroupconfigurator.cpp
    MonitorProcMem.cpp
    threadnaming.cpp
    nullvaluemanip.h
)


target_link_libraries(common 
  INTERFACE common_h
  PUBLIC
  PRIVATE
    configcpp_h 
    loggingcpp_h 
)

target_pch(common pch.h)



add_library(crashtrace STATIC
  crashtrace.cpp
)

target_link_libraries(crashtrace
  INTERFACE crashtrace_h
)