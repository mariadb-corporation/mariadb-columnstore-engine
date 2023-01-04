include_directories(BEFORE
  ${LIBMARIADB_BININC_DIR}
  ${LIBMARIADB_SRCINC_DIR})
add_definitions(-DMYSQL_SERVICE_THD_TIMEZONE_INCLUDED)

include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(querystats_LIB_SRCS querystats.cpp)

add_library(querystats SHARED ${querystats_LIB_SRCS})

add_dependencies(querystats loggingcpp)

install(TARGETS querystats DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

