include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(common_LIB_SRCS
    fixedallocator.cpp
    poolallocator.cpp
    cgroupconfigurator.cpp
    MonitorProcMem.cpp
    nullvaluemanip.cpp
    threadnaming.cpp
    string_prefixes.cpp
)

columnstore_library(common ${common_LIB_SRCS})
columnstore_link(
    common
    PRIVATE
    boost_filesystem
    configcpp
    loggingcpp
    messageqcpp
    idbdatafile
    mariadb_charset
)
