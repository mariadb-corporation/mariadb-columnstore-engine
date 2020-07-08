
include_directories( ${ENGINE_COMMON_INCLUDES} ../primproc)

########### next target ###############

set(dbbc_STAT_SRCS
    blockcacheclient.cpp
    blockrequestprocessor.cpp
    fileblockrequestqueue.cpp
    filebuffer.cpp
    filebuffermgr.cpp
    filerequest.cpp
    iomanager.cpp
    stats.cpp
    fsutils.cpp)

#libdbbc_a_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)

add_library(dbbc STATIC ${dbbc_STAT_SRCS})

add_dependencies(dbbc loggingcpp)

target_link_libraries(dbbc ${NETSNMP_LIBRARIES})

INSTALL (TARGETS dbbc DESTINATION ${ENGINE_LIBDIR})
