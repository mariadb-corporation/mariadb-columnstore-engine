INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (cacheutils_LIB_SRCS cacheutils.cpp)

ADD_LIBRARY (cacheutils SHARED ${cacheutils_LIB_SRCS})

ADD_DEPENDENCIES (cacheutils loggingcpp)

INSTALL (
  TARGETS cacheutils
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
