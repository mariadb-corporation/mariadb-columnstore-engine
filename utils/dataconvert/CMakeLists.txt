
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(dataconvert_LIB_SRCS dataconvert.cpp)

add_library(dataconvert SHARED ${dataconvert_LIB_SRCS})

set_target_properties(dataconvert PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS dataconvert DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)
