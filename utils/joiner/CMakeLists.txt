INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (joiner_LIB_SRCS tuplejoiner.cpp joinpartition.cpp)

ADD_LIBRARY (joiner SHARED ${joiner_LIB_SRCS})

ADD_DEPENDENCIES (joiner loggingcpp)

INSTALL (
  TARGETS joiner
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
