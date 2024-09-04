INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (dataconvert_LIB_SRCS dataconvert.cpp)

ADD_LIBRARY (dataconvert SHARED ${dataconvert_LIB_SRCS})

ADD_DEPENDENCIES (dataconvert loggingcpp)

INSTALL (
  TARGETS dataconvert
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
