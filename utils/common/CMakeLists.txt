
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(common_LIB_SRCS
    fixedallocator.cpp
    poolallocator.cpp
    cgroupconfigurator.cpp
    MonitorProcMem.cpp
    nullvaluemanip.cpp
    threadnaming.cpp
    utils_utf8.cpp
    statistics.cpp)

add_library(common SHARED ${common_LIB_SRCS})

target_link_libraries(common boost_filesystem)

add_dependencies(common loggingcpp)

install(TARGETS common DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

