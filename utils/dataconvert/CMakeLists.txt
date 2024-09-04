INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (dataconvert_LIB_SRCS dataconvert.cpp)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (dataconvert STATIC ${dataconvert_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (dataconvert SHARED ${dataconvert_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (dataconvert loggingcpp)

INSTALL (
  TARGETS dataconvert
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
