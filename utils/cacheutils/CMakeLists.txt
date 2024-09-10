INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (cacheutils_LIB_SRCS cacheutils.cpp)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (cacheutils STATIC ${cacheutils_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (cacheutils SHARED ${cacheutils_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (cacheutils loggingcpp)

INSTALL (
  TARGETS cacheutils
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
