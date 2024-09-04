INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (joiner_LIB_SRCS tuplejoiner.cpp joinpartition.cpp)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (joiner STATIC ${joiner_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (joiner SHARED ${joiner_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (joiner loggingcpp)

INSTALL (
  TARGETS joiner
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
