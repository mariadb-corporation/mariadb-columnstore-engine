
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(configcpp_LIB_SRCS configcpp.cpp xmlparser.cpp configstream.cpp)

add_library(configcpp SHARED ${configcpp_LIB_SRCS})

set_target_properties(configcpp PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS configcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

