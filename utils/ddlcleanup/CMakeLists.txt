include_directories(${ENGINE_COMMON_INCLUDES})

set(ddlcleanuputil_LIB_SRCS ddlcleanuputil.cpp)
columnstore_library(ddlcleanuputil ${ddlcleanuputil_LIB_SRCS})
columnstore_link(ddlcleanuputil PRIVATE loggingcpp ${NETSNMP_LIBRARIES})
