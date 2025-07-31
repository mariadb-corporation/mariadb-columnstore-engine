include_directories(BEFORE ${LIBMARIADB_BININC_DIR} ${LIBMARIADB_SRCINC_DIR})
add_definitions(-DMYSQL_SERVICE_THD_TIMEZONE_INCLUDED)

include_directories(${ENGINE_COMMON_INCLUDES})

set(querystats_LIB_SRCS querystats.cpp)
columnstore_library(querystats ${querystats_LIB_SRCS})
columnstore_link(querystats PRIVATE loggingcpp)
