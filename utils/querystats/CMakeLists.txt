
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(querystats_LIB_SRCS querystats.cpp)

add_library(querystats SHARED ${querystats_LIB_SRCS})

install(TARGETS querystats DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-libs)

