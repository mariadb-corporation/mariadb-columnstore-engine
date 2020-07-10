
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(querystats_LIB_SRCS querystats.cpp)

add_library(querystats SHARED ${querystats_LIB_SRCS})

add_dependencies(querystats loggingcpp)

install(TARGETS querystats DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

