
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(querystats_LIB_SRCS querystats.cpp)

add_library(querystats SHARED ${querystats_LIB_SRCS})

set_target_properties(querystats PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS querystats DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

