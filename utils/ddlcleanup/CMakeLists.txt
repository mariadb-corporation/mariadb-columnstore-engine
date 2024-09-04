INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (ddlcleanuputil_LIB_SRCS ddlcleanuputil.cpp)

ADD_LIBRARY (ddlcleanuputil SHARED ${ddlcleanuputil_LIB_SRCS})

ADD_DEPENDENCIES (ddlcleanuputil loggingcpp)

TARGET_LINK_LIBRARIES (ddlcleanuputil ${NETSNMP_LIBRARIES})

INSTALL (
  TARGETS ddlcleanuputil
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
