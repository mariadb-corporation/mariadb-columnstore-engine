
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
    statistics.cpp
    string_prefixes.cpp)

add_library(common STATIC ${common_LIB_SRCS})

add_dependencies(common loggingcpp)
target_link_libraries(common boost_filesystem loggingcpp mysys)



install(TARGETS common DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

