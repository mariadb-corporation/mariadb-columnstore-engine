INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (batchloader_LIB_SRCS batchloader.cpp)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (batchloader STATIC ${batchloader_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (batchloader SHARED ${batchloader_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (batchloader loggingcpp)

TARGET_LINK_LIBRARIES (batchloader ${NETSNMP_LIBRARIES})

INSTALL (
  TARGETS batchloader
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
