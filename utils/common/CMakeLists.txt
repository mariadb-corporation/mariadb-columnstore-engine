
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
    emptyvaluemanip.cpp)

add_library(common SHARED ${common_LIB_SRCS})

add_dependencies(common loggingcpp)

if(USE_LIBRE2)
    add_dependencies(common libre2)
endif()

install(TARGETS common DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

