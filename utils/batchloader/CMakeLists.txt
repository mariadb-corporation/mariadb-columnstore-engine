INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (batchloader_LIB_SRCS batchloader.cpp)

ADD_LIBRARY (batchloader SHARED ${batchloader_LIB_SRCS})

ADD_DEPENDENCIES (batchloader loggingcpp)

TARGET_LINK_LIBRARIES (batchloader ${NETSNMP_LIBRARIES})

INSTALL (
  TARGETS batchloader
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
