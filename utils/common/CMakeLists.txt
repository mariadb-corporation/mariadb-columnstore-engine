
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(common_LIB_SRCS
    fixedallocator.cpp
    poolallocator.cpp
    cgroupconfigurator.cpp
    MonitorProcMem.cpp
    nullvaluemanip.cpp)

add_library(common SHARED ${common_LIB_SRCS})

add_dependencies(common loggingcpp)


set_target_properties(common PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS common DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

