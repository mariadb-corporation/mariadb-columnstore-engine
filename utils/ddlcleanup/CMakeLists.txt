INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (ddlcleanuputil_LIB_SRCS ddlcleanuputil.cpp)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (ddlcleanuputil STATIC ${ddlcleanuputil_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (ddlcleanuputil SHARED ${ddlcleanuputil_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (ddlcleanuputil loggingcpp)

TARGET_LINK_LIBRARIES (ddlcleanuputil ${NETSNMP_LIBRARIES})

INSTALL (
  TARGETS ddlcleanuputil
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
